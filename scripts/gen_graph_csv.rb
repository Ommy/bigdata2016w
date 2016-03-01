COLUMN_A = "Date"
COLUMN_B = "Shipments"
COLUMN_C = "CANADA"
COLUMN_D = "UNITED STATES"

data = File.new(ARGV[0], "r")

data_arr = []
while line = data.gets
  str_a = line.gsub("(", "").gsub(")", "").split(",")
  line = data.gets
  str_b = line.gsub("(", "").gsub(")", "").split(",")
  if str_a.include?("CANADA")
    data_arr <<  "#{str_a[0]}, #{str_a[1]}, #{str_b[1]}"
  else
    data_arr <<  "#{str_b[0]}, #{str_b[1]}, #{str_a[1]}"
  end
end

puts "#{COLUMN_A}, #{COLUMN_B}, #{COLUMN_C}, #{COLUMN_D}"
data_arr.each do |x|
  puts x
end
