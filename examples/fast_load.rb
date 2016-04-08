require 'postgres-copy'
require 'active_record'
require 'pg_data_encoder'
require 'benchmark'
# Create a test db before running
# add any needed username, password, port
# install the required gems
#
# the easiest would to install bundler
# > gem install bundler
# > bundle install
# > bundle exec fast_load.rb


ActiveRecord::Base.establish_connection(
        :adapter  => "postgresql",
        :host     => "localhost",
        :database => "test"
)
ActiveRecord::Base.connection.execute %{
  SET client_min_messages TO warning;
  DROP TABLE IF EXISTS test_models;
  CREATE TABLE test_models (id serial PRIMARY KEY, data VARCHAR);
}

class TestModel < ActiveRecord::Base
  acts_as_copy_target
end
encoder = PgDataEncoder::EncodeForCopy.new

puts "Loading data to disk"
puts Benchmark.measure {
  0.upto(1_000_000).each {|i|
    encoder.add ["test data"]
  }
}
puts "inserting into db"
puts Benchmark.measure {
  TestModel.copy_from(encoder.get_io, :format => :binary, :columns => [:data])
}

encoder.remove
# Results on my i5 with ssd backed postgres server
# 11.7 seconds to generate data file.   3.7 seconds to insert 1,000,000 simple items into a table.
#
# 11.670000   0.010000  11.680000 ( 11.733414)
#  0.030000   0.000000   0.030000 (  3.782371)



# NEW TESTS BY MICHAEL CONRAD

# Reset database and insert with .create
ActiveRecord::Base.connection.execute %{
  SET client_min_messages TO warning;
  DROP TABLE IF EXISTS test_models;
  CREATE TABLE test_models (id serial PRIMARY KEY, data VARCHAR);
}

puts "inserting into db via .create"
puts Benchmark.measure {
  TestModel.transaction do
    1_000_000.times { TestModel.create(data: "test data") }
  end
}
# Results on my 2.2 Ghz Intel Core i7 laptop connected to a postgres server.  It took 635.986367 seconds to insert 1,000,000 simple items into a table.
#
# 467.930000  30.920000 498.850000 (635.986367)



# Reset database and insert with mass insert
ActiveRecord::Base.connection.execute %{
  SET client_min_messages TO warning;
  DROP TABLE IF EXISTS test_models;
  CREATE TABLE test_models (id serial PRIMARY KEY, data VARCHAR);
}

puts "inserting into db via single mass insert"

thingy = []

puts "adding data to array"
puts Benchmark.measure {
  0.upto(1_000_000).each {|i|
    thingy.push "('test data')"
  }
}
puts "inserting into db"
puts Benchmark.measure {
  TestModel.connection.execute "INSERT INTO test_models (data) VALUES #{thingy.join(', ')}"
}
# Results on my 2.2 Ghz Intel Core i7 laptop connected to a postgres server.  It took 18.338188 seconds to insert 1,000,000 simple items into a table.
#
#   0.330000   0.020000   0.350000 (  0.351006)
#   0.140000   0.020000   0.160000 ( 18.338188)


