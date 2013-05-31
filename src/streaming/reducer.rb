current_word = nil
current_count = 0
ARGF.each do |line|
    (word, count) = line.split("\t")

    if !current_word
        current_word = word
        current_count = 1
    elsif current_word != word
        puts "#{current_word}\t#{current_count}"
        current_word = word
        current_count = 1
    else
        current_count = current_count + 1
    end
end
puts "#{current_word}\t#{current_count}"

