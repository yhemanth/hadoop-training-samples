#!/usr/bin/ruby
cur_word = nil
cur_word_count = 0 
STDIN.each do |word|
  word = word.strip
  if cur_word and cur_word != word
    puts "%s\t%d" % [cur_word,cur_word_count]
    cur_word = word
    cur_word_count = 1
  else
    cur_word = word
    cur_word_count = cur_word_count+1
  end
end
puts "%s\t%d" % [cur_word,cur_word_count]
