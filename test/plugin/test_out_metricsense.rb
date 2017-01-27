require 'fluent/test'
require 'fluent/plugin/out_metricsense'

class MetricsenseOutputTest < Test::Unit::TestCase
  class TestBackend < Fluent::MetricSenseOutput::Backend
    Fluent::MetricSenseOutput.register_backend('test', self)

    @@data = []

    def self.data
      @@data
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
  end

  def create_driver(conf)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::MetricSenseOutput.new).configure(conf)
  end

  def test_emit
    now = Time.now.to_i
    d = create_driver(CONFIG)
    data = {'value' => 1, 'user_id' => 23456, 'path' => '/auth/login'}
    d.emit(data, now)
    d.run

    t = now / 60 * 60
    TestBackend.data.each do |written|
      assert_equal ['test', t, 1, 'user_id', 23456, 0], written.shift
      assert_equal ['test', t, 1, 'path', "/auth/login", 0], written.shift
      assert_equal ['test', t, 1, nil, nil, 0], written.shift
    end
  end
end
