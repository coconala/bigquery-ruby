class BigQuery
  class QueryResult < Array
    attr_reader :total_bytes_processed

    def initialize(result)
      @total_bytes_processed = result['totalBytesProcessed'].to_i
      make_rows(result)
    end

    private
    def make_rows(res)
      schema = res['schema']['fields']
      if res['totalRows'].to_i > 0
        res['rows'].each {|row| self.push(row_hash(row, schema)) }
      end
    end

    def row_hash(row, schema)
      rh = {}
      row['f'].each_with_index do |field, index|
        name = schema[index]['name']
        rh[name] = parse_value(field['v'], schema[index]['type'])
      end
      rh
    end

    def parse_value(value, type)
      case type
      when 'STRING'
        return value
      when 'INTEGER'
        return value.to_i
      when 'FLOAT'
        return value.to_f
      when 'BOOLEAN'
        return (value.upcase == 'TRUE') ? true : false
      when 'TIMESTAMP'
        t = value.to_f.to_i
        return Time.at(t)
      else
        raise 'unknown data type: #{type}'
      end
    end
  end
end
