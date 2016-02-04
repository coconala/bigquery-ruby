class BigQuery
  class Resource
    def initialize(res)
      @resource = make_resource(res)
    end

    def to_s
      @resource['id']
    end

    def properties
      @resource.keys
    end

    def method_missing(method, *args)
      @resource[method.to_s]
    end

    private
    def make_resource(src)
      result = {}
      src.each do |k, v|
        key = k.gsub(/[A-Z]/, '_\&').downcase
        next if respond_to?(key)

        result[key] = v.kind_of?(Hash) ? BigQuery::Resource.new(v) : v
      end
      result
    end
  end
end
