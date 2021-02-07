#Processes to run
#1 find avg sentence length in corpus?
#2 make filtered feature tables for each corpora in each decade
#3 pull out a single text file per corpora per decade per target with n-features l and r
#4 use #3 and #2 to find MDCollcoates
#5 Calculate NPMI on all collcoates in n distance - avg is the "hanging together" of the terms

pmi<-function(paired.freq, target.freqs, word.y, feature.table, horizon){
  total.words<-sum(feature.table)
  total.horizons<-rowSums(feature.table)
  total.horizons<-total.horizons-(horizon-1)
  total.horizons<-sum(total.horizons)
  paired.prob<-paired.freq/total.horizons
  prob.x<-(sum(target.freqs))/total.words
  prob.y<-(sum(which(colnames(feature.table)==word.y)))/total.words
  pmi<-log(paired.prob/((prob.x)*(prob.y)))
  return(pmi)
}

npmi<-function(pmi, paired.freq, feature.table, horizon){
  total.horizons<-rowSums(feature.table)
  total.horizons<-total.horizons-(horizon-1)
  total.horizons<-sum(total.horizons)
  paired.prob<-paired.freq/total.horizons
  npmi<-pmi/(-1*(log(paired.prob)))
  return(npmi)
}


gatherCollocates<-function(target, split.text, horizon){
  target.hits<-which(split.text %in% target)
  if(length(target.hits)>0){
    starts<-target.hits-horizon
    ends<-target.hits+horizon
    bad.starts<-which(starts<1)
    if(length(bad.starts)>0){
      starts[bad.starts]<-1
    }
    bad.ends<-which(ends>length(split.text))
    if(length(bad.ends)>0){
      ends[bad.ends]<-length(split.text)
    }
    col.seq<-mapply(function(x,y) seq(x,y), starts, ends, SIMPLIFY=F)
    all.cols<-lapply(col.seq, function(x) split.text[x])
    raw.cols<-table(unlist(all.cols))
    raw.cols<-tapply(raw.cols, names(raw.cols), sum)
    raw.cols[which(names(raw.cols)==target)]<-raw.cols[which(names(raw.cols)==target)]-length(target.hits)
    all.window.cols<-lapply(all.cols, function(x) editWindow(x, target, horizon))
    all.window.cols<-unlist(all.window.cols)
    all.window.cols<-tapply(all.window.cols, names(all.window.cols), sum)
    blank.index<-which(names(all.window.cols)=="")
    if(length(blank.index)>0){
      all.window.cols<-all.window.cols[-blank.index]
    }
    raw.cols<-raw.cols[which(names(raw.cols) %in% names(all.window.cols))]
    return(list(all.window.cols,raw.cols))
  }
}

editWindow<-function(split.col.window, target, horizon){
  if(length(split.col.window)!=((horizon*2)+1)){
    deficit<-rep(NA, (((horizon*2)+1)-length(split.col.window)))
    target.pos<-which(split.col.window==target)
    if(target.pos<(horizon+1)){
      split.col.window<-c(deficit, split.col.window)
    } else {
      split.col.window<-c(split.col.window, deficit)
    }
  }
  dist.index<-c(seq((horizon+1),(horizon*2), by=1), sort(seq((horizon+1),(horizon*2),by=1), decreasing=T))
  split.col.window<-split.col.window[-(horizon+1)]
  expanded.window<-unlist(mapply(function(x,y) rep(x,y), split.col.window, dist.index))
  expanded.window<-table(expanded.window)
  return(expanded.window)
}


cleanText<-function(raw.text){
  raw.text<-unlist(strsplit(raw.text, ""))
  raw.text<-tolower(raw.text)
  keep.char<-which(raw.text %in% c(letters, " ", "\'"))
  clean.text<-raw.text[keep.char]
  clean.text<-paste(clean.text, collapse="")
  clean.text<-unlist(strsplit(clean.text, " "))
  return(clean.text)
}

pmiCollocateCorpus<-function(corpus.dir, target, horizon, term.min=5, dict=NULL){
  library(tm)
  full.corpus.names<-list.files(corpus.dir)
  full.corpus<-lapply(list.files(corpus.dir, full.names=T), function(x) scan(x, what='character', sep="\n", quiet=T, encoding="UTF-8"))
  full.corpus<-lapply(full.corpus, function(x) paste(x, collapse=" "))
  full.corpus<-lapply(full.corpus, function(x) cleanText(x))
  all.col.lists<-lapply(full.corpus, function(x) gatherCollocates(target, x, horizon))
  all.cols<-lapply(all.col.lists, function(x) x[[1]])
  all.raw.cols<-lapply(all.col.lists, function(x) x[[2]])
  all.raw.cols<-tapply(unlist(all.raw.cols), names(unlist(all.raw.cols)), sum)
  all.cols<-tapply(unlist(all.cols), names(unlist(all.cols)), sum)
  full.corpus<-Corpus(VectorSource(full.corpus))
  corpus.dtm<-DocumentTermMatrix(full.corpus, control=list(wordLengths=c(1,Inf)))
  all.cols<-all.cols[-which(names(all.cols) %in% stopwords("en"))]
  all.cols<-all.cols[which(names(all.cols) %in% colnames(corpus.dtm))]
  if(!is.null(dict)){
    all.cols<-all.cols[which(names(all.cols) %in% dict)]
  }
  all.raw.cols<-all.raw.cols[which(names(all.raw.cols) %in% names(all.cols))]
  corpus.dtm<-as.matrix(corpus.dtm)
  text.lengths<-rowSums(corpus.dtm)
  num.windows<-text.lengths-(horizon-1)
  total.num.windows<-sum(num.windows)
  n.words<-sum(rowSums(corpus.dtm))
  target.freq<-corpus.dtm[,which(colnames(corpus.dtm)==target)]
  target.total<-sum(target.freq)
  target.prob<-target.total/n.words
  col.probs<-all.cols/total.num.windows
  individual.col.freq<-lapply(names(col.probs), function(x) corpus.dtm[,which(colnames(corpus.dtm) == x)])
  individual.col.sums<-unlist(lapply(individual.col.freq, function(x) sum(x)))
  individual.col.probs<-individual.col.sums/n.words
  all.pmi<-individual.col.probs*target.prob
  all.pmi<-col.probs/all.pmi
  all.pmi<-unlist(lapply(all.pmi, function(x) log(x)))
  entropy<-lapply(col.probs, function(x) log(x))
  entropy<-lapply(entropy, function(x) -1*x)
  all.npmi<-mapply(function(x,y) x/y, all.pmi, entropy, SIMPLIFY = F)
  all.npmi<-unlist(all.npmi)
  collocate.terms<-names(all.pmi)
  pmi.table<-data.frame(collocate.terms, all.raw.cols, all.pmi, all.npmi, stringsAsFactors=F)
  colnames(pmi.table)<-c("Term", "ObsAsCollocate", "PMI", "NPMI")
  pmi.table<-pmi.table[order(pmi.table$NPMI, decreasing=T),]
  return(pmi.table)
  pmi.table<-pmi.table[-which(pmi.table$ObsAsCollocate<term.min),]
  return(pmi.table)
  # corpus.dtm<-as.matrix(corpus.dtm)
  # all.col.sums<-all.col.sums[which(names(all.col.sums) %in% colnames(corpus.dtm))]
  # all.col.sums<-all.col.sums[which(all.col.sums>=min)]
  # if(length(target)>2){
  #   target.cols<-which(colnames(corpus.dtm) %in% target)
  #   target.counts<-rowSums(corpus.dtm[,target.cols])
  # } else {
  #   target.counts<-corpus.dtm[,which(colnames(corpus.dtm)==target)]
  # }
  # test.list<-list(all.col.sums, corpus.dtm)
  # pmis<-mapply(function(x,y) pmi(x, target.counts, y, corpus.dtm, horizon), all.col.sums, names(all.col.sums), SIMPLIFY = F)
  # npmis<-mapply(function(x,y) npmi(x, y, corpus.dtm, horizon), pmis, all.col.sums, SIMPLIFY = F)
  # results<-data.frame(names(all.col.sums), all.col.sums, unlist(pmis), unlist(npmis), stringsAsFactors = F)
  # colnames(results)<-c("Term", "Freq", "PMI", "Norm_PMI")
  # results<-results[order(results$Norm_PMI, decreasing=T),]
  # return(results)
}