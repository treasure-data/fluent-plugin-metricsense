# encoding: ascii-8bit
#
# fluent-plugin-metricsense
#
# Copyright (C) 2012 Sadayuki Furuhashi
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
module Fluent

  class MetricSenseOutput < Fluent::BufferedOutput
    Fluent::Plugin.register_output('metricsense', self)

    BACKENDS = {}

    class Backend
      include Configurable

      def start
      end

      def shutdown
      end
    end

    backend_dir = "#{File.dirname(__FILE__)}/backend"
    Dir.glob("#{backend_dir}/*_backend.rb") {|e|
      require e
    }

    config_param :segment_keys, :string, :default => nil
    config_param :all_segment, :bool, :default => false
    config_param :value_key, :string, :default => 'value'

    config_param :backend, :string

    config_param :remove_tag_prefix, :string, :default => nil

    def configure(conf)
      super

      if @remove_tag_prefix
        @remove_tag_prefix = Regexp.new('^' + Regexp.escape(@remove_tag_prefix) + "\\.?")
      end

      if conf.has_key?('all_segment') && conf['all_segment'].empty?
        @all_segment = true
      end

      if @all_segment
        @segment_keys = nil
      elsif @segment_keys
        @segment_keys = @segment_keys.strip.split(/\s*,\s*/)
      else
        @segment_keys = []
      end

      be = BACKENDS[@backend]
      unless be
        raise ConfigError, "unknown backend: #{@backend.inspect}"
      end

      @backend = be.new
      @backend.configure(conf)
    end

    def start
      @backend.start
      super
    end

    def shutdown
      super
      @backend.shutdown
    end

    def format_stream(tag, es)
      out = ''
      es.each do |time,record|
        value = record[@value_key]

        fv = value.to_f
        next if fv == 0.0

        iv = fv.to_i
        if iv.to_f == fv
          value = iv
        else
          value = fv
        end

        seg_keys = @segment_keys
        unless seg_keys
          seg_keys = record.keys
          seg_keys.delete(@value_key)
        end

        segs = []
        seg_keys.each {|seg_key|
          if seg_val = record[seg_key]
            segs << seg_key
            segs << seg_val
          end
        }

        tag.sub!(@remove_tag_prefix, '') if @remove_tag_prefix

        [tag, time, value, segs].to_msgpack(out)
      end
      out
    end

    class SumAggregator
      def initialize
        @value = 0
      end

      def add(value)
        @value += value
      end

      attr_reader :value
    end

    AggregationKey = Struct.new(:tag, :time, :seg_val, :seg_key)

    def write(chunk)
      counters = {}

      # select sum(value) from chunk group by tag, time/60, seg_val, seg_key
      chunk.msgpack_each {|tag,time,value,segs|
        time = time / 60 * 60

        ak = AggregationKey.new(tag, time, nil, nil)
        (counters[ak] ||= SumAggregator.new).add(value)

        segs.each_slice(2) {|seg_key,seg_val|
          ak = AggregationKey.new(tag, time, seg_val, seg_key)
          (counters[ak] ||= SumAggregator.new).add(value)
        }
      }

      data = []
      counters.each_pair {|ak,aggr|
        data << [ak.tag, ak.time, aggr.value, ak.seg_key, ak.seg_val]
      }

      @backend.write(data)
    end
  end

end
