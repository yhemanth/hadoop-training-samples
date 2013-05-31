ARGF.each do |line|
    words = line.split
    words.each do |word|
        puts "#{word}\t1"
    end
end