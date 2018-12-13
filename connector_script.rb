require 'peddler'
require 'awesome_print'
require 'net/http'
require 'json'

load 'secrets.rb'

CLIENT = MWS::Orders::Client.new(
  primary_marketplace_id: MARKETPLACE_ID,
  merchant_id:            MERCHANT_ID,
  aws_access_key_id:      AWS_ACCESS_KEY_ID,
  aws_secret_access_key:  AWS_SECRET_ACCESS_KEY
)

TIME_PERIOD = (Time.now.utc - (60 * 60 * 24 * 30)).iso8601
FILTER      = { created_after: TIME_PERIOD, order_status: %w(Unshipped PartiallyShipped) }
API_URI     = ''
API_HEADER  = { 'Api-User': API_USER, 'Api-Pass': API_PASS }

def order_list
  orders     = []
  next_token = nil

  loop do
    response = next_token.nil? ? CLIENT.list_orders(FILTER).parse : CLIENT.list_orders_by_next_token(next_token).parse
    next_token      = response['NextToken']
    received_orders = response['Orders']
    break if received_orders.nil?

    if received_orders['Order'].is_a?(Array)
      orders += received_orders['Order']
    else
      orders << received_orders['Order']
    end

    break if next_token.nil?
    sleep 60
  end

  orders
end

def item_list(amazon_order_id)
  order_items = []
  next_token  = nil

  loop do
    items = next_token.nil? ? CLIENT.list_order_items(amazon_order_id).parse : CLIENT.list_order_items_by_next_token(next_token).parse
    next_token     = items['NextToken']
    received_items = items['OrderItems']
    break if received_items.nil?

    if received_items['OrderItem'].is_a?(Array)
      order_items += received_items['OrderItem']
    else
      order_items << received_items['OrderItem']
    end

    break if next_token.nil?
    sleep 2
  end

  order_items
end

def order_exist?(amazon_order_id)
  uri    = URI(API_URI)
  params = { route: 'order/find_by_amazon_order_id', store_id: 1, amazon_order_id: amazon_order_id }

  Net::HTTP.start(uri.host, uri.port) do |http|
    uri.query  = URI.encode_www_form(params)
    response   = http.get(uri, API_HEADER)
    order_info = JSON.parse(response.body)

    raise 'Invalid user data' unless order_info['success']
    order_info['result']
  end
end

orders      = order_list
orders_data = []

begin
  orders.each do |order|
    order_id = order['AmazonOrderId']
    next if order_exist?(order_id)

    order_items      = []
    items            = item_list(order_id)
    address          = order['ShippingAddress']
    shipping_address = {
      name:            address['Name'],
      phone:           address['Phone'],
      postal_code:     address['PostalCode'],
      country_code:    address['CountryCode'],
      state_or_region: address['StateOrRegion'],
      city:            address['City'],
      address_line_1:  address['AddressLine1'],
      address_line_2:  address['AddressLine2']
    }

    price, shipping_price = 0, 0

    items.each do |item|
      total_price     = item['ItemPrice']['Amount'].to_f
      quantity        = item['QuantityOrdered'].to_f
      shipping_price += item['ShippingPrice']['Amount'].to_f
      price          += total_price

      order_items << {
        id:       item['OrderItemId'],
        name:     item['Title'],
        model:    item['SellerSKU'],
        total:    total_price,
        quantity: quantity,
        price:   (total_price / quantity)
      }
    end

    orders_data << {
      amazon_order_id:  order_id,
      shipping_address: shipping_address,
      items:            order_items,
      total:            price,
      shipping_price:   shipping_price
    }
  end
rescue => e
  puts e.message
  puts e.backtrace
end

ap orders_data
