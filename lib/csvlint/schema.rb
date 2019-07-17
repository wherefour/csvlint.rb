module Csvlint

  class Schema

    include Csvlint::ErrorCollector

    attr_reader :uri, :fields, :fields_by_index, :title, :description

    def initialize(uri, fields=[], title=nil, description=nil)
      @uri = uri
      @fields = fields
      @title = title
      @description = description
      @fields_by_index = {}
      reset
    end

    class << self

      extend Gem::Deprecate

      def from_json_table(uri, json)
        fields = []
        json["fields"].each do |field_desc|
          fields << Csvlint::Field.new( field_desc["name"] , field_desc["constraints"],
            field_desc["title"], field_desc["description"] )
        end if json["fields"]
        return Schema.new( uri , fields, json["title"], json["description"] )
      end

      def from_csvw_metadata(uri, json)
        return Csvlint::Csvw::TableGroup.from_json(uri, json)
      end

      # Deprecated method signature
      def load_from_json(uri, output_errors = true)
        load_from_uri(uri, output_errors)
      end
      deprecate :load_from_json, :load_from_uri, 2018, 1

      def load_from_uri(uri, output_errors = true)
        load_from_string(uri, open(uri).read, output_errors)
      rescue OpenURI::HTTPError, Errno::ENOENT => e
        raise e
      end

      def load_from_string(uri, string, output_errors = true)
        begin
          json = JSON.parse( string )
          if json["@context"]
            uri = "file:#{File.expand_path(uri)}" unless uri.to_s =~ /^http(s)?/
            return Schema.from_csvw_metadata(uri,json)
          else
            return Schema.from_json_table(uri,json)
          end
        rescue TypeError => e
          # NO IDEA what this was even trying to do - SP 20160526

        rescue Csvlint::Csvw::MetadataError => e
          raise e
        rescue => e
          if output_errors === true
            STDERR.puts e.class
            STDERR.puts e.message
            STDERR.puts e.backtrace
          end
          return Schema.new(nil, [], "malformed", "malformed")
        end
      end

    end

    def validate_header(header, source_url=nil, validate=true)
      reset
      header.each_with_index do |name, i|
        field = fields.find { |field| field.name.downcase == name.downcase }

        if fields[i] && fields[i].constraints.fetch('required', nil) && fields[i].name.downcase != name.downcase
          build_errors(:missing_column, :schema, nil, fields[i].name)
        end

        if field
          @fields_by_index[i] = field
          build_warnings(:different_index_header, :schema, nil, i+1, name) if fields[i].name && fields[i].name.downcase != name.downcase
        else
          if fields[i] && fields[i].constraints.fetch('required', nil)
            build_errors(:missing_column, :schema, nil, fields[i].name)
          else
            build_warnings(:extra_column, :schema, nil, i+1, name)
          end
          build_warnings(:extra_column, :schema, nil, i+1, name)
        end
      end

      (fields - fields_by_index.values).each do |field|
        build_warnings(:missing_column, :schema, nil, fields.index(field)+1, field.name)
      end

      valid?
    end

    def validate_row(values, row=nil, all_errors=[], source_url=nil, validate=true)
      reset

      values_array = Array.new(values.length) { |i| nil }
      fields_by_index.each_with_index {|f, i| values_array[i] = (values[i] ? values[i] : nil)}

      values_array.each_with_index do |value,i|
        field = fields_by_index[i]
        if field
          result = field.validate_column(value || "", row, fields_by_index.key(field)+1)
          @errors += field.errors
          @warnings += field.warnings
        else
          build_warnings(:extra_column, :schema, row, i)
        end
      end

      fields.each_with_index do |field, i|
        build_warnings(:missing_column, :schema, row, i+1, field.name) if values_array[i].nil?
      end

      valid?
    end

  end
end
