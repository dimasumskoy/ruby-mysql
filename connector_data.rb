require 'mysql2'
require 'awesome_print'

load 'secrets.rb'

class Row
  CLIENT = Mysql2::Client.new(
    host:      'localhost',
    username:  MYSQL_USER,
    password:  MYSQL_PASS,
    port:      3306,
    database:  'connector_test'
  )

  CLIENT.query_options.merge!(symbolize_keys: true)

  @table = 'data'

  class << self
    attr_reader :table

    def find(id)
      CLIENT.query("SELECT * FROM #{table} WHERE id = #{id}").first
    end

    def create(data = {})
      fields, values = [], []

      data.each do |field, value|
        fields << field
        value.nil? && value = :null
        value.is_a?(String) ? values << "'#{CLIENT.escape(value)}'" : values << "#{value}"
      end

      query_fields = fields.join(', ')
      query_values = values.join(', ')

      CLIENT.query("INSERT INTO #{table} (#{query_fields}) VALUES (#{query_values})")
      CLIENT.query("SELECT MAX(id) FROM #{table}").first[:"MAX(id)"]
    end

    def create_bulk(data = [])
      fields, values, converted_values = [], [], []

      data.first.each_key { |field| fields << field }
      data.each           { |row|   values << row.values }

      values.each do |row|
        row.map! { |value| convert_value(value) }
        converted_values << "(#{row.join(', ')})"
      end

      query_fields = fields.join(', ')
      query_values = converted_values.join(', ')

      CLIENT.query("INSERT INTO #{table} (#{query_fields}) VALUES #{query_values}")
    end

    def update(id, data = {})
      initial_row    = find(id)
      converted_data = []

      data.each do |field, value|
        value = convert_value(value)
        converted_data << "#{field} = #{value}"
      end

      query_data = converted_data.join(', ')
      query_time = "'#{Time.now.strftime('%Y-%m-%d %H:%M:%S')}'"

      CLIENT.query("UPDATE #{table} SET #{query_data}, updated_at = #{query_time} WHERE id = #{id}")

      changed_row = find(id)

      unless changed_row == initial_row
        CLIENT.query("UPDATE #{table} SET is_changed = true WHERE id = #{id}")
      end
    end

    def reset_is_changed
      CLIENT.query("UPDATE #{table} SET is_changed = false")
    end

    private

    def convert_value(value)
      value.nil? && value = :null
      value.is_a?(String) ? value = "'#{CLIENT.escape(value)}'" : value
    end
  end
end

