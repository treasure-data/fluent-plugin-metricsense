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

  class MetricSenseOutput < Fluent::Plugin::Output
    Fluent::Plugin.register_output('metricsense', self)

    helpers :compat_parameters

    def formatted_to_msgpack_binary?
      true
    end

    def compat_parameters_default_chunk_key
      ""
    end

    BACKENDS = {}

    def self.register_backend(name, klass)
      BACKENDS[name] = klass
    end

    module UpdateMode
      ADD = 0
      MAX = 1
      AVERAGE = 2
      COUNT = 3
    end

    class Backend
      UpdateMode = MetricSenseOutput::UpdateMode
      include Fluent::Configurable

      attr_accessor :log

      def start
      end
      def shutdown
      end
    end

    module Backends
      backend_dir = "#{File.dirname(__FILE__)}/backends"
      require "#{backend_dir}/datadog_backend"
      require "#{backend_dir}/librato_backend"
      require "#{backend_dir}/rdb_tsdb_backend"
      require "#{backend_dir}/stdout_backend"
    end

    config_param :value_key, :string, :default => 'value'

    config_param :no_segment_keys, :bool, :default => false
    config_param :only_segment_keys, :string, :default => nil
    config_param :exclude_segment_keys, :string, :default => nil

    config_param :update_mode_key, :string, :default => 'update_mode'

    config_param :remove_tag_prefix, :string, :default => nil
    config_param :add_tag_prefix, :string, :default => nil

    config_param :backend, :string

    config_param :aggregate_interval, :time, :default => 60

    def configure(conf)
      compat_parameters_convert(conf, :buffer)
      super

      if @remove_tag_prefix
        @remove_tag_prefix = Regexp.new('^' + Regexp.escape(@remove_tag_prefix) + "\\.?")
      end

      @no_segment_keys = (conf.has_key?('no_segment_keys') && (conf['no_segment_keys'].empty? || conf['no_segment_keys'] == 'true'))

      if @only_segment_keys
        @only_segment_keys = @only_segment_keys.strip.split(/\s*,\s*/)
      end

      if @exclude_segment_keys
        @exclude_segment_keys = @exclude_segment_keys.strip.split(/\s*,\s*/)
      end

      be = BACKENDS[@backend]
      unless be
        raise ConfigError, "unknown backend: #{@backend.inspect}"
      end

      # aggregate_interval must be a multiple of 60 to normalize values
      # into X per minute
      @aggregate_interval = @aggregate_interval.to_i / 60 * 60
      @normalize_factor = @aggregate_interval / 60

      @backend = be.new
      @backend.log = log
      @backend.configure(conf)
    end

    def start
      @backend.start
      super
    end

    def shutdown
      @backend.shutdown
      super
    end

    def format(tag, time, record)
      tag = tag.sub(@remove_tag_prefix, '') if @remove_tag_prefix
      tag = "#{@add_tag_prefix}.#{tag}" if @add_tag_prefix

      record = record.dup

      value = record.delete(@value_key)
      return nil if value.nil?

      begin
        fv = value.to_f
      rescue
        return nil
      end
      return nil if fv.nan? || fv.infinite?

      iv = fv.to_i
      value = iv.to_f == fv ? iv : fv

      update_mode = record.delete(@update_mode_key)
      case update_mode
      when "max"
        update_mode = UpdateMode::MAX
      when "average"
        update_mode = UpdateMode::AVERAGE
      when "count"
        update_mode = UpdateMode::COUNT
      else
        update_mode = UpdateMode::ADD
      end

      if @no_segment_keys
        segments = {}
      else
        if @only_segment_keys
          segments = {}
          @only_segment_keys.each {|key|
            if v = record[key]
              segments[key] = v
            end
          }
        else
          segments = record
        end
        if @exclude_segment_keys
          @exclude_segment_keys.each {|key|
            segments.delete(key)
          }
        end
      end

      [tag, time.to_i, value, segments, update_mode].to_msgpack
    end

    class AddUpdater
      def initialize
        @value = 0
      end
      attr_reader :value

      def normalized_value(n)
        n == 1 ? @value : @value.to_f / n
      end

      def add(value)
        @value += value
      end

      def mode
        UpdateMode::ADD
      end
    end

    class CountUpdater
      def initialize
        @value = 0
      end
      attr_reader :value

      def normalized_value(n)
        @value
      end

      def add(value)
        @value += 1
      end

      def mode
        UpdateMode::ADD
      end
    end

    class MaxUpdater
      def initialize
        @value = 0
      end
      attr_reader :value

      def normalized_value(n)
        @value
      end

      def add(value)
        if @value < value
          @value = value
        end
        @value
      end

      def mode
        UpdateMode::MAX
      end
    end

    class AverageUpdater < MaxUpdater
      def mode
        UpdateMode::AVERAGE
      end
    end

    AggregationKey = Struct.new(:tag, :time, :seg_val, :seg_key)

    def write(chunk)
      simple_counters = {}
      segmented_counters = {}

      # select sum(value) from chunk group by tag, time/60, seg_val, seg_key
      chunk.msgpack_each {|tag,time,value,segments,update_mode|
        begin
          time = time / @aggregate_interval * @aggregate_interval

          case update_mode
          when UpdateMode::ADD
            updater = AddUpdater
          when UpdateMode::MAX
            updater = MaxUpdater
          when UpdateMode::AVERAGE # AVERAGE uses MaxUpdater and calculate average on server-side aggregation
            updater = AverageUpdater
          when UpdateMode::COUNT
            updater = CountUpdater
          else  # default is AddUpdater
            updater = AddUpdater
          end

          # simple values
          ak = AggregationKey.new(tag, time, nil, nil)
          (simple_counters[ak] ||= updater.new).add(value)

          # segmented values
          segments.each_pair {|seg_key,seg_val|
            ak = AggregationKey.new(tag, time, seg_val, seg_key)
            (segmented_counters[ak] ||= updater.new).add(value)
          }
        rescue StandardError => e
          log.warn("ignoring broken chunk: #{e.inspect} - " + [tag, time, value, segments, update_mode].inspect)
        end
      }

      counters = segmented_counters
      counters.merge!(simple_counters)

      data = []
      counters.each_pair {|ak,up|
        data << [ak.tag, ak.time, up.normalized_value(@normalize_factor), ak.seg_key, ak.seg_val, up.mode]
      }

      @backend.write(data)
    end
  end

end
