pairs_file = File.new(ARGV[0], "r")
stripes_file = File.new(ARGV[1], "r")

pairs_output = {}
stripes_output = {}

while line = pairs_file.gets
  str = line.split("\t")
  pairs_output[str[0]] = str[1]
end

while line = stripes_file.gets
  str = line.split("\t")
  stripes_output[str[0]] = str[1]
end

# pairs_output = Hash[pairs_output.sort_by{|k,v| k}]
# stripes_output = Hash[stripes_output.sort_by{|k,v|k}]
floatV = 0
theirV = 0
pairs_output.each do |k,v|
  unless (stripes_output.has_key?(k))
    puts "Key #{k} not found in stripes"
    next
  end
  floatV = (v.to_f * 100.0).floor / 100.00
  theirV = (stripes_output[k].to_f * 100.0).floor / 100.00
  unless floatV == theirV
    puts "Mismatch on key #{k}. Pairs returns #{v} but Stripes returns #{stripes_output[k]}"
  end
end