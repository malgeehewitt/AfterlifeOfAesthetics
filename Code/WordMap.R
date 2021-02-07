#quick and dirty code for dividing texts into arbitrary lengths, measuring the percentage 
#of a wordlist in each segment

qdTextClean<-function(text.unclean){
  #split the single text into characters and unlist it
  text.split<-unlist(strsplit(text.unclean, ""))
  #finds which characters in the text are lowercase, uppercase letters or spaces
  text.letter_and_space.index<-which(text.split %in% c(letters, LETTERS, " "))
  #retrieve the actual letters and spaces from the text
  text.clean<-text.split[text.letter_and_space.index]
  #turn all upper case letters into lower case letter equivalents
  text.clean<-tolower(text.clean)
  #paste the letters and spaces back together into a single text object
  text.clean<-paste(text.clean, collapse="")
  #split the text into words by splitting on spaces
  text.clean<-unlist(strsplit(text.clean, " "))
  #find words with nothing in them
  empty.space<-which(text.clean=="")
  #check and see if there are any empty space
  if(length(empty.space)>0){
    #if so, remove them
    text.clean<-text.clean[-empty.space]
  }
  #return the individual words as a vector
  return(text.clean)
}


qdMeasure<-function(text.filename, wordlist, seg.length){
  text.content<-scan(text.filename, what='character', sep="\n")
  text.content<-paste(text.content, " ")
  text.clean<-qdTextClean(text.content)
  seg.starts<-seq(1, length(text.clean), seg.length)
  seg.ends<-seg.starts[2:length(seg.starts)]
  seg.ends<-c(seg.ends, length(text.clean))
  text.segs<-mapply(function(x,y) text.clean[x:y], seg.starts, seg.ends, SIMPLIFY = F)
  seg.nums<-seq(1, length(text.segs), by=1)
  seg.lengths<-lapply(text.segs, function(x) length(x))
  wordlist.rawcount<-lapply(text.segs, function(x) length(which(x %in% wordlist)))
  seg.lengths<-unlist(seg.lengths)
  wordlist.rawcount<-unlist(wordlist.rawcount)
  wordlist.perc<-(wordlist.rawcount/seg.lengths)*100
  stat.table<-data.frame(seg.nums, seg.lengths, wordlist.rawcount, wordlist.perc)
  return(stat.table)
}


