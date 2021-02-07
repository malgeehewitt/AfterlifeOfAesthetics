#Text cleaning code
cleanText<-function(raw.text){
  split.text<-unlist(strsplit(raw.text, ""))
  split.text<-gsub("-", " ", split.text)
  split.text<-tolower(split.text)
  split.text<-split.text[which(split.text %in% c(letters, " "))]
  split.text<-paste(split.text, collapse="")
  clean.text<-unlist(strsplit(split.text, " "))
  empty.words<-which(clean.text=="")
  if(length(empty.words)>0){
    clean.text<-clean.text[-empty.words]
  }
  remove(split.text)
  gc()
  return(clean.text)
}

#code to smooth plot by a factor
smoothPlot<-function(seg.probs, smoothing.factor){
  if(length(seg.probs)<=smoothing.factor){
    return(seg.probs)
  } else {
    smoothing.half<-ceiling(((smoothing.factor-1)/2))
    seg.index<-seq(1, length(seg.probs), by=1)
    smoothing.indicies<-lapply(seg.index, function(x) seq((x-smoothing.half), (x+smoothing.half), by=1))
    smoothing.indicies<-lapply(smoothing.indicies, function(x) x[which(x %in% seg.index)])
    smoothing.vector<-lapply(smoothing.indicies, function(x) seg.probs[x])
    smoothed.vector<-lapply(smoothing.vector, function(x) mean(x))
    smoothed.vector<-unlist(smoothed.vector)
    return(smoothed.vector)
  }
}


#code to divide the text into evenly spaced overlapping windows
splitTextWindow<-function(split.text, window.size, overlap, percentage=F){
  text.length<-length(split.text)
  if(percentage){
    window.size<-floor(text.length*(window.size/100))
    overlap<-floor(text.length*(overlap/100))
  }
  window.starts<-seq(1, text.length, by=overlap)
  window.ends<-window.starts+window.size
  excess.ends<-which(window.ends>text.length)
  if(length(excess.ends)>1){
    window.ends[excess.ends]<-text.length
  }
  all.sizes<-window.ends-window.starts
  if(all.sizes[length(all.sizes)]<(overlap-1)){
    window.starts<-window.starts[-length(window.starts)]
    window.ends<-window.ends[-length(window.ends)]
  }

  text.segments<-mapply(function(x,y) split.text[x:y], window.starts, window.ends, SIMPLIFY=F)
  return(text.segments)
}

#code to print the percentage of text from a wordlest per segment as a line graph over the length of the text
printGraph<-function(data.table, text.name, target, output.folder){
  library(ggplot2)
  graph.name<-paste("Plot of",  target, "in", text.name, sep=" ")
  graph.filename<-paste("Plot_of", target, "in", text.name, sep="_")
  graph.filename<-paste(graph.filename, ".pdf", sep="")
  graph.filename<-paste(output.folder, graph.filename, sep="/")
  seg.graph<-ggplot(data.table, aes(x=Segment, y=Percent_Hits))+geom_line()+ggtitle(graph.filename)
  pdf(graph.filename, height=5, width=15)
  print(seg.graph)
  dev.off()
}

#code to calculate the percentage of words from a wordlist in each segment of a divided text
textSegPercentage<-function(text.filename, text.segments, wordlist, target.name, output.folder, smooth.plot=NULL, window.name){
  #print(c(text.filename, target.name))
  raw.hits<-lapply(text.segments, function(x) length(which(x %in% wordlist)))
  total.tokens<-lapply(text.segments, function(x) length(x))
  raw.hits<-unlist(raw.hits)
  total.tokens<-unlist(total.tokens)
  segment.probs<-raw.hits/total.tokens
  mean.seg.probs<-mean(segment.probs)
  upper.sd<-mean.seg.probs+(sd(segment.probs))
  lower.sd<-mean.seg.probs-(sd(segment.probs))
  if(!is.null(smooth.plot)){
    segment.probs<-smoothPlot(segment.probs, smooth.plot)
    smooth.name<-as.character(smooth.plot)
  } else {
    smooth.name<-"NS"
  }
  text.name<-unlist(strsplit(text.filename, "/"))
  text.name<-text.name[length(text.name)]
  text.name<-unlist(strsplit(text.name, ".txt"))
  bound.segments<-unlist(lapply(text.segments, function(x) paste(x, collapse=" ")))
  text.table<-data.frame(rep(text.name, length(segment.probs)), seq(1,length(segment.probs), by=1), segment.probs, rep(target.name, length(segment.probs)), rep(mean.seg.probs, length(segment.probs)), rep(upper.sd, length(segment.probs)), rep(lower.sd, length(segment.probs)), rep(window.name, length(segment.probs)), rep(smooth.name, (length(segment.probs))), bound.segments, stringsAsFactors=F)
  colnames(text.table)<-c("Text", "Segment", "Percent_Hits", "Target", "Mean", "SDev_Above", "SDev_Below", "Window", "Smoothed", "Segment_Text")
  table.name<-paste("Table_of", target.name, "in", text.name, sep="_")
  table.name<-paste(table.name, ".csv", sep="")
  table.name<-paste(output.folder, table.name, sep="/")
  write.csv(text.table, table.name, row.names=F)
  printGraph(text.table, text.name, target.name, output.folder)
  return(text.table)
}

#code to import, clean, and segment text before counting target hits in each segment
calculateTargetLists<-function(text.filename, target.wordlists, target.names, output.folder, window.size, window.advance, percentage=F, smooth.plot=NULL){
  raw.text<-scan(text.filename, what='character', sep="\n", quiet=T)
  raw.text<-paste(raw.text, collapse=" ")
  clean.text<-cleanText(raw.text)
  text.segments<-splitTextWindow(clean.text, window.size, window.advance, percentage)
  #target.tables<-mapply(function(x,y) textSegPercentage(text.filename, text.segments, x, y, output.folder, window.size, window.advance, percentage), target.wordlists, target.names, SIMPLIFY=T)
  #final.table<-do.call("rbind", target.tables)
  if(percentage){
    window.name<-paste("Perc", as.character(window.size), sep="")
  } else {
    window.name<-as.character(window.size)
  }
  final.table<-textSegPercentage(text.filename, text.segments, target.wordlists, target.names, output.folder, smooth.plot, window.name)
  return(final.table)
}

#code to iterate the targetwordlist counting function across a folder of text files
#NOTE: Target wordlists needs to be a list of vectors (even if that list only has one member)
graphDiscourseFolder<-function(source.folder, target.wordlists, target.names, output.folder, window.size, window.advance, percentage=F, smooth.plot=NULL){
  full.text.list<-list.files(source.folder, full.names=T, pattern=".txt")
  all.text.graphs<-lapply(full.text.list, function(x) calculateTargetLists(x, target.wordlists, target.names, output.folder, window.size, window.advance, percentage, smooth.plot))
  final.table<-do.call("rbind", all.text.graphs)
  write.csv(final.table, file=paste(output.folder, "FullHitsStatsTable.csv", sep="/"), row.names=F)
  return(final.table)
}

#code to iterate GraphDiscourseFolder over a series of parameters
#each new combination of parameters will be placed in a new directory (these can be collected with the code below)
#the parameters include: window size (percent or not), wordlist, smoothing
#NOTE: wordlists and targets need to be the same length, as do window.sizes, window.advances, and percentage
variableIterate<-function(source.folder, wordlists, target.list.names, output.folder.root, window.sizes, window.advances, percentage, smoothing.factors){
  for(i in 1:length(wordlists)){
    for(j in 1:length(window.sizes)){
      if(percentage[j]){
        window.name<-paste("Perc", as.character(window.sizes[j], sep=""))
      } else {
        window.name<-as.character(window.sizes[j])
      }
      for(k in 1:length(smoothing.factors)){
        if(smoothing.factors[k]==0){
          smooth.name<-"NS"
          curr.smooth<-NULL
        } else {
          smooth.name<-as.character(smoothing.factors[k])
          curr.smooth<-smoothing.factors[k]
        }
        folder.name<-paste(target.list.names[i], window.name, smooth.name, sep="_")
        folder.name<-paste(output.folder.root, folder.name, sep="/")
        print(folder.name)
        folder.create<-ifelse(!dir.exists(folder.name), dir.create(folder.name), FALSE)
        temp.table<-graphDiscourseFolder(source.folder, wordlists[[i]], target.list.names[i], folder.name, window.sizes[j], window.advances[j], percentage[j], curr.smooth)
      }
    }
  }
}


#Function to combine all individual tables (with different divisions and smoothing) into a single one
#full.text indicates whether the full text of the segment is included in the combined table (cuts down on file size)
tableCombine<-function(root.folder, full.text=F){
  all.table.filenames<-list.files(root.folder, recursive=T, full.names=T, pattern="FullHits")
  all.table.names<-lapply(all.table.filenames, function(x) unlist(strsplit(x, "/")))
  all.table.names<-unlist(lapply(all.table.names, function(x) x[2]))
  all.tables<-lapply(all.table.filenames, function(x) read.csv(x, header=T, stringsAsFactors=F))
  if(!full.text){
    all.tables<-lapply(all.tables, function(x) x[-ncol(x)])
  }
  variable.cols<-unlist(mapply(function(x,y) rep(x,nrow(y)), all.table.names, all.tables, SIMPLIFY = F))
  combined.table<-do.call("rbind", all.tables)
  combined.table$Measurement<-variable.cols
  write.csv(combined.table, file=paste(root.folder, "CombinedResultsTables.csv", sep="/"), row.names=F)
  return(combined.table)
}


############################
#Code to create color html documents indicating the presence of sublime words
colorCodeText<-function(split.text, text.color, word.list){
  match.index<-which(split.text %in% word.list)
  if(length(match.index)>0){
    word.hits<-split.text[match.index]
    tinted.text<-unlist(lapply(word.hits, function(x) paste('<font color="', text.color, '">', x, '</font>', sep="")))
    split.text[match.index]<-tinted.text
  }
  collapsed.text<-paste(split.text, collapse=" ")
  return(collapsed.text)
}

wordlistTextColor<-function(word.list, text.color, text.filename){
  raw.text<-scan(text.filename, what='character', sep="\n", quiet=T)
  raw.text<-unlist(lapply(raw.text, function(x) paste(x, "linebreak", sep=" ")))
  raw.text<-paste(raw.text, collapse=" ")
  clean.text<-cleanText(raw.text)
  tinted.text<-colorCodeText(clean.text, text.color, word.list)
  tinted.text<-gsub("linebreak", "<br>", tinted.text)
  return(tinted.text)
}

colorFolder<-function(input.folder, output.folder, text.color="#ff0ff", wordlist){
  #color.table<-matrix(c("$", "#BE3D5A", "CC", "#F777DA", "CD", "#F90368", "DT", "#F903BD", "EX", "#2ED675", "IN", "#DA4586", "JJ", "#05FA0D", "JJR", "#186C08", "JJS", "#3EA75F", "MD", "#4411F2", "NN","#F71304", "NNP", "#CE0515", "NNPS", "#D4434E", "NNS", "#EA573B", "PDT", "#889505", "PRP", "#FCA008", "PRP$", "#FA8E04", "RB", "#9D11F2", "RBR", "#622487", "RBS", "#B157CA", "RP", "#BED610", "TO", "#B2F90F", "UH", "#6E890A", "VB", "#3BE5EA", "VBD", "#0614F0", "VBG", "#2F6FF7", "VBN", "#01B5FD", "VBP", "#039FC5", "VBZ", "#2D64F7", "WDT", "#FCDB08", "WP", "#DB741E", "WP$", "#DB5F0E", "WRB", "#924DAC"), ncol=2, byrow=T)
  all.files<-list.files(input.folder, full.names = T)
  all.file.names<-list.files(input.folder)
  all.file.names<-lapply(all.file.names, function(x) unlist(strsplit(x, ".txt")))
  all.file.names<-lapply(all.file.names, function(x) paste(output.folder, "/", x, "_SublimeWords.html", sep=""))
  all.tints<-lapply(all.files, function(x) wordlistTextColor(wordlist, text.color, x))
  final.texts<-lapply(all.tints, function(x) paste("<!DOCTYPE HTML>\n", "<html>\n", "<body>\n", x, "</p>", "</body>\n", "</html>", sep=" "))
  mapply(function(x,y) write(x,y), final.texts, all.file.names, SIMPLIFY = F)
}
