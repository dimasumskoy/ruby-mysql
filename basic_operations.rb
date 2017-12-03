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
        values << convert_value(value)
      end

      query_fields = fields.join(', ')
      query_values = values.join(', ')

      CLIENT.query("INSERT INTO #{table} (#{query_fields}) VALUES (#{query_values})")
      CLIENT.query("SELECT MAX(id) FROM #{table}").first[:"MAX(id)"]
    end

    def create_bulk(data = [])
      fields, values, converted_values = [], [], []

      data.each { |row| row.each_key { |key| fields << key } }

      fields.uniq!
      
      data.each do |row|
        row_values = []
        fields.each { |field| row.has_key?(field) ? row_values << row[field] : row_values << nil }
        values << row_values
      end

      values.each do |row|
        row.map! { |value| convert_value(value) }
        converted_values << "(#{row.join(', ')})"
      end

      query_fields = fields.join(', ')
      query_values = converted_values.join(', ')

      CLIENT.query("INSERT INTO #{table} (#{query_fields}) VALUES #{query_values}")
    end

    def update(id, data = {})
      initial_row = find_by_fields(id, data)

      unless initial_row == data
        converted_data = []
        data.each { |field, value| converted_data << "#{field} = #{convert_value(value)}" }

        query_data = converted_data.join(', ')
        query_time = "'#{Time.now.strftime('%Y-%m-%d %H:%M:%S')}'"

        CLIENT.query("UPDATE #{table} SET #{query_data}, updated_at = #{query_time}, is_changed = true WHERE id = #{id}")
      end
    end

    def reset_is_changed
      CLIENT.query("UPDATE #{table} SET is_changed = false")
    end

    def find_by_fields(id, data = {})
      fields = []
      data.each_key { |field| fields << field }

      CLIENT.query("SELECT #{fields.join(', ')} FROM #{table} WHERE id = #{id}").first
    end

    private

    def convert_value(value)
      case value
      when NilClass then :null
      when String   then "'#{CLIENT.escape(value)}'"
      else value
      end
    end
  end
end

