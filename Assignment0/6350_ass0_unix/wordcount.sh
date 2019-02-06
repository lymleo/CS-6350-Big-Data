wget https://www.gutenberg.org/files/98/98-0.txt
cat 98-0.txt |tr -cs "[:alnum:]" "[\012*]"|tr A-Z a-z|sort|uniq -c|sort -k1nr -k2|tee yxl160531Part1.txt
