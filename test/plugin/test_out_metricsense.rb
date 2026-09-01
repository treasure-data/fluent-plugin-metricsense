require 'test-unit'
require 'fluent/test'
require 'fluent/test/driver/output'
require 'fluent/test/helpers'
require 'fluent/plugin/out_metricsense'

class MetricsenseOutputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  class TestBackend < Fluent::MetricSenseOutput::Backend
    Fluent::MetricSenseOutput.register_backend('test', self)

    @@data = []

    def self.data
      @@data
    end

    def self.clear
      @@data = []
    end

    def write(data)
      @@data << data
    end
  end

  CONFIG = %Q[
    backend test
  ]

  def setup
    Fluent::Test.setup
    TestBackend.clear
  end

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::MetricSenseOutput).configure(conf)
  end

  def test_emit
    now = Time.now.to_i
    d = create_driver
    d.run(default_tag: 'test') do
      d.feed(now, {'value' => 1, 'user_id' => 23456, 'path' => '/auth/login'})
    end

    t = now / 60 * 60
    assert_true TestBackend.data.length > 0
    TestBackend.data.each do |written|
      assert_equal ['test', t, 1, 'user_id', 23456, 0], written.shift
      assert_equal ['test', t, 1, 'path', "/auth/login", 0], written.shift
      assert_equal ['test', t, 1, nil, nil, 0], written.shift
    end
  end

  # server restart can cause broken buffer and broken chunk.
  # out_metricsense should ignore the chunk with warning log.
  # Note: v1 driver validates inputs, so we hand-roll a corrupt chunk
  # to exercise write's error handling directly.
  def test_skip_broken_chunk
    d = create_driver

    corrupt_chunk = Object.new
    def corrupt_chunk.msgpack_each
      yield ['test', 'not_a_number', 1, {}, 0]
    end

    assert_nothing_raised do
      d.instance.write(corrupt_chunk)
    end
    assert_equal [[]], TestBackend.data
  end

  def test_format_skips_nil_value
    now = Time.now.to_i
    d = create_driver
    d.run(default_tag: 'test') do
      d.feed(now, {'no_value_key' => 'x'})
      d.feed(now, {'value' => 1})
    end

    assert_nil d.formatted.first
    assert_equal 1, d.formatted.compact.length
    assert_equal 1, TestBackend.data.flatten(1).length
  end

  def test_format_skips_nan_value
    now = Time.now.to_i
    d = create_driver
    d.run(default_tag: 'test') do
      d.feed(now, {'value' => Float::NAN})
      d.feed(now, {'value' => 1})
    end

    assert_nil d.formatted.first
    assert_equal 1, d.formatted.compact.length
    assert_equal 1, TestBackend.data.flatten(1).length
  end

  def test_format_skips_infinite_value
    now = Time.now.to_i
    d = create_driver
    d.run(default_tag: 'test') do
      d.feed(now, {'value' => Float::INFINITY})
      d.feed(now, {'value' => 1})
    end

    assert_nil d.formatted.first
    assert_equal 1, d.formatted.compact.length
    assert_equal 1, TestBackend.data.flatten(1).length
  end

  def test_reloadable
    d = create_driver
    assert_true d.instance.reloadable_plugin?
  end
end
