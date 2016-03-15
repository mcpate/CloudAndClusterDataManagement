#!/usr/bin/ruby

require 'csv'
require 'riak'

# finds the highway by primary key and builds a hash of its fields
def buildHighwayHash(highway_pk, highway_rows)
  highway_rows.each do |row|
    if row[0] == highway_pk
      highway_data = {}
      highway_data[:highwayid] = row[0]
      highway_data[:shortdirection] = row[1]
      highway_data[:direction] = row[2]
      highway_data[:highwayname] = row[3]
      return highway_data
    end
  end
end

# Find all detectors that have the station_pk and build a list of hashes
# (One hash for each dector in the station)
def buildDetectorHashArray(station_pk, detector_rows)
  detector_rows.each do |row|
    if row[6] ==  station_pk
      detector_data = {}
      detector_data[:detectorid] = row[0]
      detector_data[:detectorclass] = row[4]
      detector_data[:lanenumber] = row[5]
      return detector_data
    end
  end
end

#
# Start!
#
# pull all highways into memory
highway_rows = CSV.read("/tmp/highways.csv")

# pull all stations into memory
station_rows = CSV.read("/tmp/freeway_stations.csv")

# pull all detectors into memory
detector_rows = CSV.read("/tmp/freeway_detectors.csv")

# get the client
client = Riak::Client.new(:pb_port => 8087)

# get the bucket we're going to add all stations to
puts "Creating stations bucket..."
#stations_index = client.create_search_index("stations_idx");
stations_bucket = client.bucket("stations")
#stations_bucket.properties = {"search_index" => "stations_idx"}

# build and add all stations
puts "Inserting stations..."
num_inserted = 0
station_rows.each do |row|
  station_data = {}
  station_data[:stationid] = row[0]
  station_data[:highway] = buildHighwayHash(row[1], highway_rows)
  station_data[:milepost] = row[2]
  station_data[:locationtext] = row[3]
  station_data[:upstream] = row[4]
  station_data[:downstream] = row[5]
  station_data[:stationclass] = row[6]
  station_data[:numberlanes] = row[7]
  station_data[:latlon] = row[8]
  station_data[:length] = row[9]
  station_data[:detectors] = buildDetectorHashArray(row[0], detector_rows)

  new_station = stations_bucket.new(row[0].to_s)
  new_station.data = station_data
  new_station.store()
  num_inserted = num_inserted + 1
end
puts "Done. " + num_inserted.to_s + " k/v pairs inserted."

# get the bucket we're putting all loopdata in
puts "Creating loopdata bucket..."
#loopdata_index = client.create_search_index("loopdata_idx")
loopdata_bucket = client.bucket("loopdata")
#loopdata_bucket.properties = {"search_index" => "loopdata_idx"}

# read through loopdata one row at a time and insert
puts "Inserting loopdata..."
num_inserted = 0
CSV.foreach("/tmp/freeway_loopdata.csv") do |row|
  loopdata_data = {}
  loopdata_data[:detectorid] = row[0]
  loopdata_data[:starttime] = row[1]
  loopdata_data[:volume] = row[2]
  loopdata_data[:speed] = row[3]
  loopdata_data[:occupancy] = row[4]
  loopdata_data[:status] = row[5]
  loopdata_data[:dqflags] = row[6]

  # let random key be generated here
  new_loopdata = loopdata_bucket.new()
  new_loopdata.data = loopdata_data
  new_loopdata.store()
  num_inserted = num_inserted + 1
end
puts "Done. " + num_inserted.to_s + " k/v pairs inserted."
puts "Finished."
