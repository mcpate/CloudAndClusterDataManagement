# information on creating secondary indexes
http://basho.github.io/riak-ruby-client/2i.html
http://docs.basho.com/riak/latest/dev/using/2i/


client = Riak::Client.new(:pb_port => 8087)

# listing buckets
client.buckets

# storing a station
stations_bucket = client.bucket("stations")

# listing keys currently in bucket
stations_bucket.keys

station_data = {
  :stationId => 1045,
  :milepost => 14.32,
  :locationText => "Sunnyside NB",
  :upstream => 0,
  :downstream => 1046,
  :stationClass => 1,
  :numberLanes => 4,
  :latLon => "45.43324,-122.565775",
  :length => 0.94,
  :highway => {
    :highwayId => 3,
    :shortDirection => "NORTH",
    :highwayName => "I-205"
  },
  :dectectorIds => [1345, 1346, 1347, 1348]
}

new_station = stations_bucket.new("1045")
new_station.data = station_data
new_station.store()

# storing loopdata for one of the above stations
loopdata_bucket = client.bucket("loopdata")

# index on detectorId, index on startDay, index on startTime, index on speed
detector_data = {
  :detectorId => 1345,
  :startDay => "09/15/11",
  :startTime => "0:00",
  :volume => 0,
  :speed => 66,
  :occupancy => 0,
  :status => 0,
  :dqflags => 0
}

# auto-assigned key
detector_with_random_key = loopdata_bucket.new()
detector_with_random_key.data = detector_data
detector_with_random_key.store()

# getting station
stations_bucket = client.buckets("stations")
stations_buckket["1045"]

# deleting buckets
stations_bucket.delete()
