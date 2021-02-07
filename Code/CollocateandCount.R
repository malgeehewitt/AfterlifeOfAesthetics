#code to build frequency table of filtered words from a corpus
cleanText<-function(raw.text){
  split.text<-gsub("-", " ", split.text)
  split.text<-tolower(split.text)
  split.text<-split.text[which(split.text %in% c(letters, " "))]
  split.text<-paste(split.text, collapse="")
  clean.text<-unlist(strsplit(split.text, " "))
  remove(split.text)
  gc()
  return(clean.text)
}

textCounts<-function(text.fn, dictionary=NULL){#}, ptm){
  #ptm<-proc.time()
  test.scan<-tryCatch(raw.text<-scan(text.fn, what='character', sep="\n", quiet=T), error=function(e){return("NotFound")})
  if(length(test.scan)>0){
    if(test.scan=="NotFound"){
      return(text.fn)
     } else {
      raw.text<-paste(raw.text, collapse=" ")
      #print(proc.time()-ptm)
      clean.text<-cleanText(raw.text)
      remove(raw.text)
      #print(proc.time()-ptm)
      if(!is.null(dictionary)){
        clean.text<-clean.text[which(clean.text %in% dictionary)]
      }
      #print(proc.time()-ptm)
      clean.text<-table(clean.text)
      blank.index<-which(names(clean.text)=="")
      if(length(blank.index)>0){
        clean.text<-clean.text[-blank.index]
      }
      #print(proc.time()-ptm)
      empty.dict<-rep(0, length(dictionary))
      names(empty.dict)<-dictionary
      #empty.dict<-sort(empty.dict)
      #clean.text<-sort(clean.text)
      empty.dict[which(names(empty.dict) %in% names(clean.text))]<-clean.text
      #clean.text<-c(clean.text, empty.dict)
      #clean.text<-tapply(clean.text, names(clean.text), sum)
      #print(proc.time()-ptm)
      #return(clean.text)
      return(empty.dict)
     }
  }
}

makeFreqTable<-function(meta.table, filename.col="Filename", dictionary=NULL, output.folder, corpus.name, year.char){
  #ptm<-proc.time()
  #make the new folder
  output.subfolder<-paste(output.folder, year.char, sep="/")
  folder.create<-ifelse(!dir.exists(output.subfolder), dir.create(output.subfolder), FALSE)
  
  #extract the paths from the metadata table
  all.paths<-meta.table[,filename.col]
  
  #start cluster
  size<-mpi.universe.size()
  np <- if (size >1 ) size - 1 else 1
  cluster<-makeCluster(np, type='MPI', outfile='')
  
  #spread variables (dictionary, filenames, textCounts function) and return list with LB apply
  clusterExport(cluster, list("dictionary"))
  all.tables<-ClusterApplyLB(cl=cluster, x=all.paths, fun=textCounts)
  stopCluster(cluster)
  
  #all.tables<-lapply(meta.table[,filename.col], function(x) textCounts(x,dictionary))#,ptm))
  
  all.tables<-unlist(all.tables)
  final.table<-matrix(all.tables, ncol=length(dictionary), byrow=T)
  colnames(final.table)<-dictionary
  #write the table to the folder
  table.fn<-paste(corpus.name, "frequencytable.csv", sep="_")
  table.output.path<-paste(output.subfolder, table.fn, sep="/")
  write.csv(final.table, table.output.path, row.names=F)
}

#dict.path<-"/scratch/groups/malgeehe/data/mark/code/EditedDictionary.txt"
#output.folder<-"/scratch/groups/malgeehe/data/mark/project/sublime/Test/"
#meta.table<-"/scratch/groups/malgeehe/data/mark/project/sublime/SublimeMeta.csv"

writeFreqTableYear<-function(sub.meta, dictionary, output.folder, corpus.name, clust.obj, filename.col){
  print(dim(sub.meta))
  year<-unique(sub.meta$year_fixed)
  
  #make the new folder
  output.subfolder<-paste(output.folder, as.character(year), sep="/")
  folder.create<-ifelse(!dir.exists(output.subfolder), dir.create(output.subfolder), FALSE)
  
  all.paths<-sub.meta[,filename.col]
  ptm<-proc.time()
  #sub.tables<-lapply(all.paths, function(x) textCounts(x, dictionary))
  sub.tables<-parLapply(cl=clust.obj, all.paths, function(x) textCounts(x, dictionary))
  #error control
  all.lengths<-unlist(lapply(sub.tables, function(x) length(x)))
  errors<-which(all.lengths==1)
  if(length(errors)>1){
    print("Errors:")
    print(sub.tables[errors])
    sub.tables<-sub.tables[-errors]
  }
  print(proc.time()-ptm)
  
  
  #bind tables into a final matrix
  sub.tables<-unlist(sub.tables)
  final.table<-matrix(sub.tables, ncol=length(dictionary), byrow=T)
  remove(sub.tables)
  gc()
  colnames(final.table)<-dictionary
  
  #write the table to the folder
  table.fn<-paste(corpus.name, "frequencytable.csv", sep="_")
  table.output.path<-paste(output.subfolder, table.fn, sep="/")
  write.csv(final.table, table.output.path, row.names=F)
  remove(final.table)
  gc()
}


#function to build indiviudal frequency tables per corpus per year
buildYearTablesMPI<-function(full.meta.table, dictionary, output.folder.top, filename.col="fn"){
  #ptm<-proc.time()
  #library(Rmpi)
  library(parallel)
  
  #extract the paths from the metadata table
  #all.paths<-full.meta.table[,filename.col]
  
  #start cluster
  #size<-mpi.universe.size()
  #np <- if (size >1 ) size - 1 else 1
  #cluster<-makeCluster(np, type='MPI', outfile='')
  size<-detectCores()
  np<- if (size>1) size-1 else 1
  print(np)
  proc.clust<-makeCluster(16, type="FORK")
  
  #spread variables (dictionary, filenames, textCounts function) and return list of word counts per text with load balanced apply
  #clusterExport(cluster, list("dictionary", "textCounts"))
  #all.tables<-parLapply(cl=proc.clust, all.paths, function(x) textCounts(x, dictionary))
  
  
  unique.corpora<-unique(full.meta.table$corpus)
  
  for (i in 1:length(unique.corpora)){
    curr.corpus<-unique.corpora[i]
    print(curr.corpus)
    corpus.index<-which(full.meta.table$corpus==curr.corpus)
    #sub.corpus.tables<-all.tables[corpus.index]
    sub.meta.table<-full.meta.table[corpus.index,]
    unique.years<-unique(sub.meta.table$year_fixed)
    print(unique.years)
    completed.years<-completedFiles(curr.corpus, output.folder.top)
    if(!is.na(completed.years[1])){
      completed.years<-as.numeric(completed.years)
      unique.years<-unique.years[-which(unique.years %in% completed.years)]
    }
    print(length(unique.years))
    if(length(unique.years)>0){
      sub.tables<-lapply(unique.years, function(x) sub.meta.table[which(sub.meta.table$year_fixed==x),])
      #parLapply(proc.clust, sub.tables, function(x) writeFreqTableYear(x, dictionary, output.folder.top, curr.corpus, filename.col))
      lapply(sub.tables, function(x) writeFreqTableYear(x, dictionary, output.folder.top, curr.corpus, proc.clust, filename.col))
    }
  }
  stopCluster(proc.clust)
  #print(proc.time()-ptm)
}

#code to find files that are done for exclusion
completedFiles<-function(curr.corpus, folder.top){
  curr.corpus<-paste(curr.corpus, "frequencytable.csv", sep="_")
  all.files<-list.files(folder.top, recursive=T, full.names=T, pattern=curr.corpus)
  if(length(all.files)>0){
    all.files.split<-lapply(all.files, function(x) unlist(strsplit(x, "/")))
    corpus.years<-unlist(lapply(all.files.split, function(x) x[(length(x)-1)]))
    return(corpus.years)
  } else {
    return(NA)
  }
}


#######################################
#The following code builds a dictionary by finding the unique words in a group of texts
textWords<-function(text.filename, dictionary){
  raw.text<-scan(text.filename, what='character', sep="\n", quiet=T)
  raw.text<-paste(raw.text, sep=" ")
  raw.text<-cleanText(raw.text)
  raw.text<-raw.text[which(raw.text %in% dictionary)]
  raw.text<-unique(raw.text)
  return(raw.text)
}


buildDictionary<-function(full.meta.table, dictionary, output.folder, filename.col="fn"){
  #ptm<-proc.time()
  #library(Rmpi)
  library(parallel)
  
  #extract the paths from the metadata table
  #all.paths<-full.meta.table[,filename.col]
  
  #start cluster
  #size<-mpi.universe.size()
  #np <- if (size >1 ) size - 1 else 1
  #cluster<-makeCluster(np, type='MPI', outfile='')
  # size<-detectCores()
  # np<- if (size>1) size-1 else 1
  # print(np)
  # proc.clust<-makeCluster(16, type="FORK")
  
  
  unique.corpora<-unique(full.meta.table$corpus)
  
  all.words<-list()
  
  for (i in 1:length(unique.corpora)){
    curr.corpus<-unique.corpora[i]
    print(curr.corpus)
    corpus.index<-which(full.meta.table$corpus==curr.corpus)
    #sub.corpus.tables<-all.tables[corpus.index]
    sub.meta.table<-full.meta.table[corpus.index,]
    sub.files<-sub.meta.table[,filename.col]
    text.words<-lapply(sub.files, function(x) textWords(x, dictionary))
    text.words<-unlist(text.words)
    text.words<-unique(text.words)
    all.words<-c(all.words, list(text.words))
  }
  all.words<-unlist(all.words)
  all.words<-unique(all.words)
  all.words<-as.data.frame(all.words, stringsAsFactors=F)
  output.filename<-paste(output.folder, "DictionaryTerms.csv", sep="/")
  write.csv(all.words, output.filename, row.names=F)
  #stopCluster(proc.clust)
  #print(proc.time()-ptm)
}




#######################################
#the following code finds the average sentence lengths per year
avgSentText<-function(path, sent.anno, word.anno){
  raw.text<-scan(path, what='character', sep="\n", quiet=T)
  raw.text<-paste(raw.text, collapse=" ")
  raw.text<-as.String(raw.text)
  all.sent<-annotate(raw.text, sent.anno)
  all.word<-annotate(raw.text, word.anno, all.sent)
  n.words<-length(subset(all.word, type=="word"))
  avg.sent<-n.words/length(all.sent)
  remove(raw.text)
  remove(all.sent)
  remove(all.word)
  gc()
  return(avg.sent)
}

getYearSentAvg<-function(sub.meta, filename.col){
  sent.anno<-Maxent_Sent_Token_Annotator()
  word.anno<-Maxent_Word_Token_Annotator()
  
  year<-unique(sub.meta$year_fixed)
  all.paths<-sub.meta[,filename.col]
  avg.lengths<-lapply(all.paths, function(x) avgSentText(x, sent.anno, word.anno))
  avg.lengths<-unlist(avg.lengths)
  avg.lengths<-mean(avg.lengths)
  avg.lengths<-c(year, avg.lengths)
  remove(sent_anno)
  remove(word_anno)
  gc()
  return(avg.lengths)
}

sentenceLengths<-function(meta.table, output.folder, filename.col="fn"){
  library(parallel)
  library(openNLP)
  library(openNLPdata)
  library(NLP)

  
  size<-detectCores()
  np<- if (size>1) size-1 else 1
  print(np)
  proc.clust<-makeCluster(16, type="FORK")
  
  unique.corpora<-unique(meta.table$corpus)
  
  for (i in 1:length(unique.corpora)){
    curr.corpus<-unique.corpora[i]
    print(curr.corpus)
    corpus.index<-which(meta.table$corpus==curr.corpus)
    #sub.corpus.tables<-all.tables[corpus.index]
    sub.meta.table<-meta.table[corpus.index,]
    unique.years<-unique(sub.meta.table$year_fixed)
    print(unique.years)
    if(length(unique.years)>0){
      sub.tables<-lapply(unique.years, function(x) sub.meta.table[which(sub.meta.table$year_fixed==x),])
      year.avg<-parLapply(proc.clust, sub.tables, function(x) getYearSentAvg(x, filename.col))
      #year.avg<-lapply(sub.tables, function(x) getYearSentAvg(x, sent_anno, word_anno, filename.col))
      corpus.avg<-do.call("rbind", year.avg)
      corpus.filename<-paste(curr.corpus, "AvgSentLengthperYear.csv", sep="_")
      corpus.filename<-paste(output.folder, corpus.filename, sep="/")
      write.csv(corpus.avg, file=corpus.filename, row.names=F)
      #mapply(function(x,y) writeFreqTableYear(x, dictionary, output.folder, curr.corpus, proc.clust, filename.col, y), sub.tables, unique.years, SIMPLIFY=F)
    }
  }
  stopCluster(proc.clust)
  #print(proc.time()-ptm)
}
  



#######################################
#the following code uses features of the above code to gather collocates, count them per year (both for pmi windows and absolute counts)
gatherCollocates<-function(target, split.text, horizon){
  target.hits<-which(split.text %in% target)
  if(length(target.hits)>0){
    hit.counts<-split.text[target.hits]
    hit.adjust<-table(hit.counts)
    hit.adjust<-hit.adjust*-1
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
    #There is a problem here: instead of subtracting the actual collocates found from each target, it subtracts the total of ALL target cognates from each one
    #raw.cols[which(names(raw.cols) %in% target)]<-raw.cols[which(names(raw.cols) %in% target)]-length(target.hits)
    raw.cols[which(names(raw.cols) %in% names(hit.adjust))]<-raw.cols[which(names(raw.cols) %in% names(hit.adjust))]+hit.adjust[which(names(hit.adjust) %in% names(raw.cols))]
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
  else{
     return(NA)
  }
}

editWindow<-function(split.col.window, target, horizon){
  if(length(split.col.window)!=((horizon*2)+1)){
    deficit<-rep(NA, (((horizon*2)+1)-length(split.col.window)))
    target.pos<-which(split.col.window %in% target)
    target.pos<-target.pos[1]
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

collocateCounts<-function(text.fn, dictionary=NULL, target.list, horizon){
  #ptm<-proc.time()
  test.scan<-tryCatch(raw.text<-scan(text.fn, what='character', sep="\n", quiet=T), error=function(e){return("NotFound")})
  if(length(test.scan)>0){
    if(test.scan=="NotFound"){
      return(NA)
    } else {
      raw.text<-paste(raw.text, collapse=" ")
      #print(proc.time()-ptm)
      clean.text<-cleanText(raw.text)
      remove(raw.text)
      #print(proc.time()-ptm)
      if(!is.null(dictionary)){
        clean.text<-clean.text[which(clean.text %in% dictionary)]
      }
      #print(proc.time()-ptm)
      collocate.tables<-lapply(target.list, function(x) gatherCollocates(x, clean.text, horizon))
      remove(test.scan)
      gc()
      return(collocate.tables)
    }
  }
}


writeCollocateTablesYear<-function(sub.meta, dictionary, output.folder, corpus.name, filename.col, target.list, horizon){
  #print(dim(sub.meta))
  year<-unique(sub.meta$year_fixed)
  print(year)
  
  #make the new folder
  output.subfolder<-paste(output.folder, as.character(year), sep="/")
  folder.create<-ifelse(!dir.exists(output.subfolder), dir.create(output.subfolder), FALSE)
  
  all.paths<-sub.meta[,filename.col]
  ptm<-proc.time()
  collocate.tables<-lapply(all.paths, function(x) collocateCounts(x, dictionary, target.list, horizon))
  target.tables<-list()
  for(i in 1:length(target.list)){
    all.target.tables<-lapply(collocate.tables, function(x) x[i])
    print(length(all.target.tables))
    all.window.collocates<-lapply(all.target.tables, function(x) x[[1]][1])
    all.window.collocates<-unlist(all.window.collocates)
    bad.index<-which(is.na(all.window.collocates))
    if(length(bad.index)!=length(all.window.collocates)){
      all.raw.collocates<-lapply(all.target.tables, function(x) x[[1]][2])
      all.raw.collocates<-unlist(all.raw.collocates)
      if(length(bad.index)>0){
        all.window.collocates<-all.window.collocates[-bad.index]
        all.raw.collocates<-all.raw.collocates[-bad.index]
      }
      all.window.collocates<-tapply(all.window.collocates, names(all.window.collocates), sum)
      all.raw.collocates<-tapply(all.raw.collocates, names(all.raw.collocates), sum)
      final.table<-data.frame(names(all.window.collocates), all.window.collocates, all.raw.collocates, stringsAsFactors=F)
      colnames(final.table)<-c("Term", "WindowCols", "RawCols")
      print(dim(final.table))
      print(target.list[[i]])
      output.filename<-paste(corpus.name, "_", target.list[[i]][1], "_Collocates.csv", sep="")
      output.filename<-paste(output.subfolder, output.filename, sep="/")
      print(output.filename)
      write.csv(final.table, output.filename, row.names=F)
    }
  }
  remove(collocate.tables)
  gc()
}

#function to build indiviudal collocate tables per corpus per year
buildCollocateTables<-function(full.meta.table, dictionary, output.folder.top, target.list, horizon, filename.col="fn"){
  #ptm<-proc.time()
  #library(Rmpi)
  library(parallel)
  
  #extract the paths from the metadata table
  #all.paths<-full.meta.table[,filename.col]
  
  #start cluster
  #size<-mpi.universe.size()
  #np <- if (size >1 ) size - 1 else 1
  #cluster<-makeCluster(np, type='MPI', outfile='')
  size<-detectCores()
  np<- if (size>1) size-1 else 1
  print(np)
  proc.clust<-makeCluster(np, type="FORK")
  
  #spread variables (dictionary, filenames, textCounts function) and return list of word counts per text with load balanced apply
  #clusterExport(cluster, list("dictionary", "textCounts"))
  #all.tables<-parLapply(cl=proc.clust, all.paths, function(x) textCounts(x, dictionary))
  
  
  unique.corpora<-unique(full.meta.table$corpus)
  print(unique.corpora)
  
  for (i in 1:length(unique.corpora)){
    curr.corpus<-unique.corpora[i]
    print(curr.corpus)
    corpus.index<-which(full.meta.table$corpus==curr.corpus)
    #sub.corpus.tables<-all.tables[corpus.index]
    sub.meta.table<-full.meta.table[corpus.index,]
    unique.years<-unique(sub.meta.table$year_fixed)
    print(unique.years)
    #completed.years<-completedCollocateFiles(curr.corpus, output.folder.top, target.list)
    #if(!is.na(completed.years[1])){
    #  completed.years<-as.numeric(completed.years)
    #  unique.years<-unique.years[-which(unique.years %in% completed.years)]
    #}
    print(length(unique.years))
    if(length(unique.years)>0){
      sub.tables<-lapply(unique.years, function(x) sub.meta.table[which(sub.meta.table$year_fixed==x),])
      parLapply(proc.clust, sub.tables, function(x) writeCollocateTablesYear(x, dictionary, output.folder.top, curr.corpus, filename.col, target.list, horizon))
      #lapply(sub.tables, function(x) writeCollocateTablesYear(x, dictionary, output.folder.top, curr.corpus, filename.col, target.list, horizon))
      #mapply(function(x,y) writeFreqTableYear(x, dictionary, output.folder, curr.corpus, proc.clust, filename.col, y), sub.tables, unique.years, SIMPLIFY=F)
    }
  }
  stopCluster(proc.clust)
  #print(proc.time()-ptm)
}

#code to find files that are done for exclusion
completedCollocateFiles<-function(curr.corpus, folder.top, target.list){
  curr.corpus<-lapply(target.list, function(x) paste(curr.corpus, x[1], "Collocates.csv", sep="_"))
  #print(curr.corpus)
  all.files<-lapply(curr.corpus, function(x) list.files(folder.top, recursive=T, full.names=T, pattern=x))
  all.files<-unlist(all.files)
  if(length(all.files)>0){
    all.files.split<-lapply(all.files, function(x) unlist(strsplit(x, "/")))
    corpus.years<-unlist(lapply(all.files.split, function(x) x[(length(x)-1)]))
    corpus.years<-unique(corpus.years)
    return(corpus.years)
  } else {
    return(NA)
  }
}


################
#Code for summing the yearly feature tables for each corpus and creating a single composite table with one line for each year
#also calculates the number of n=21 windows per year and the number of texts
importFreqTable<-function(file.name){
  freq.build<-scan(file.name, what="character", sep="\n", quiet=T)
  freq.build<-lapply(freq.build, function(x) unlist(strsplit(x, ",")))
  all.lengths<-lapply(freq.build, function(x) length(x))
  col.length<-all.lengths[[1]]
  short.rows<-which(all.lengths<col.length)
  if(length(short.rows)>0){
    freq.build<-freq.build[-short.rows]
  }
  freq.cols<-freq.build[[1]]
  freq.build<-freq.build[-1]
  freq.build<-unlist(freq.build)
  freq.build<-gsub("\"", "", freq.build)
  freq.build<-as.numeric(freq.build)
  na.index<-which(is.na(freq.build))
  if(length(na.index)>0){
    replacement.values<-rep(0,length(na.index))
    freq.build<-freq.build[-na.index]
    freq.build<-c(freq.build, replacement.values)
  }
  freq.build<-matrix(freq.build, ncol=length(freq.cols), byrow=T)
  freq.cols<-gsub("\"","",freq.cols)
  colnames(freq.build)<-freq.cols
  return(freq.build)
}

tableSum<-function(file.name){
  print(file.name)
  freq.table<-importFreqTable(file.name)
  freq.sum<-colSums(freq.table)
  word.totals<-rowSums(freq.table)
  n.texts<-length(word.totals)
  n.windows<-word.totals-19
  bad.windows<-which(n.windows<1)
  if(length(bad.windows)>0){
    n.windows[bad.windows]<-1
  }
  n.windows<-sum(n.windows)
  n.words<-sum(word.totals)
  summary.features<-c(n.texts, n.windows, n.words)
  return.list<-list(freq.sum, summary.features)
  return(return.list)
}

yearData<-function(folder.name, dict){
  all.freq.files<-list.files(folder.name, full.names=T, pattern="frequencytable.csv")
  table.data<-lapply(all.freq.files, function(x) tableSum(x))
  table.frequencies<-lapply(table.data, function(x) x[[1]])
  table.summary.features<-lapply(table.data, function(x) x[[2]])
  table.frequencies<-unlist(table.frequencies)
  table.frequencies<-matrix(table.frequencies, ncol=length(dict), byrow=T)
  table.frequencies<-colSums(table.frequencies)
  table.summary.features<-do.call("rbind", table.summary.features)
  table.summary.features<-colSums(table.summary.features)
  year.list<-list(table.frequencies, table.summary.features)
  return(year.list)
}

summarizeTables<-function(meta.table, source.folder.root, output.folder, dict){
  ptm<-proc.time()
  library(parallel)
  size<-detectCores()
  np<- if (size>1) size-1 else 1
  print(np)
  proc.clust<-makeCluster(16, type="FORK")
  
  unique.years<-unique(meta.table$year_fixed)
  unique.years<-as.character(unique.years)
  source.folders<-paste(source.folder.root, unique.years, sep="/")
  
  #distribute folders to the cluster
  #full.composite.data<-lapply(source.folders, function(x) yearData(x, dict))
  full.composite.data<-parLapply(proc.clust, source.folders, function(x) yearData(x, dict))
  
  stopCluster(proc.clust)
  
  #gather the results
  full.composite.freqs<-lapply(full.composite.data, function(x) x[1])
  full.composite.freqs<-matrix(unlist(full.composite.freqs), ncol=length(dict), byrow=T)
  colnames(full.composite.freqs)<-dict
  all.word.sums<-colSums(full.composite.freqs)
  #zero.index<-which(all.word.sums==0)
  #if(length(zero.index)>0){
  #  full.composite.freqs<-full.composite.freqs[,-zero.index]
  #}
  print(dim(full.composite.freqs))
  rownames(full.composite.freqs)<-unique.years
  full.composite.freqs<-full.composite.freqs[order(rownames(full.composite.freqs)),]
  summary.stats<-lapply(full.composite.data, function(x) x[[2]])
  summary.stats<-do.call("rbind", summary.stats)
  print(dim(summary.stats))
  colnames(summary.stats)<-c("Num_Texts", "Num_21Word_Windows", "TotalWords")
  print(proc.time()-ptm)
  freq.filename<-paste(output.folder, "FullCompositeFreqTable.csv", sep="/")
  write.csv(full.composite.freqs, freq.filename, row.names=F)
  summary.stat.filename<-paste(output.folder, "FullSummaryStatistics.csv", sep="/")
  write.csv(summary.stats, summary.stat.filename, row.names=F)
}


#performs the above using loops and outputting one file per corpus per year
summarizeTablesSerial<-function(meta.table, source.folder.root, output.folder, dict){
  ptm<-proc.time()
  
  unique.years<-unique(meta.table$year_fixed)
  unique.years<-as.character(unique.years)
  source.folders<-paste(source.folder.root, unique.years, sep="/")
  
  all.summary.stats<-list()
  for(i in 1:length(source.folders)){
    all.freq.files<-list.files(source.folders, full.names=T, pattern="frequencytable.csv")
    curr.summary.stats<-list()
    for(j in 1:length(all.freq.files)){
      print(all.freq.files[j])
      freq.table<-importFreqTable(all.freq.files[j])
      freq.sum<-colSums(freq.table)
      output.name<-unlist(strsplit(all.freq.files[j], ".csv"))
      output.name<-paste(output.name, "_Sums.csv")
      write.csv(freq.sum, output.name, row.names=F)
      word.totals<-rowSums(freq.table)
      n.texts<-length(word.totals)
      n.windows<-word.totals-19
      bad.windows<-which(n.windows<1)
      if(length(bad.windows)>0){
        n.windows[bad.windows]<-1
      }
      n.windows<-sum(n.windows)
      n.words<-sum(word.totals)
      summary.features<-c(n.texts, n.windows, n.words)
      curr.summary.stats<-c(curr.summary.stats, list(summary.features))
      remove(freq.table)
      remove(freq.sum)
      gc()
    }
    year.summary.stats<-do.call("rbind", curr.summary.stats)
    year.summary.stats<-rowSums(year.summary.stats)
    all.summary.stats<-c(all.summary.stats, list(year.summary.stats))
  }
  all.summary.stats<-do.call("rbind", all.summary.stats)
  rownames(all.summary.stats)<-unique.years
  all.summary.stats<-all.summary.stats[order(rownames(all.summary.stats)),]
  summary.stat.filename<-paste(output.folder, "FullSummaryStatistics.csv", sep="/")
  write.csv(all.summary.stats, summary.stat.filename, row.names=T)
}

######################
#code for collecting Collocates from year folders
tableToList<-function(collocate.table){
  all.windows<-collocate.table$WindowCols
  all.raw<-collocate.table$RawCols
  names(all.windows)<-collocate.table$Term
  names(all.raw)<-collocate.table$Term
  return(list(all.windows, all.raw))
}

sumCollocates<-function(collocate.files){
  all.collocate.tables<-lapply(collocate.files, function(x) read.csv(x, header=T, stringsAsFactors=F))
  table.lists<-lapply(all.collocate.tables, function(x) tableToList(x))
  window.cols<-lapply(table.lists, function(x) x[[1]])
  raw.cols<-lapply(table.lists, function(x) x[[2]])
  window.cols<-unlist(window.cols)
  window.cols<-tapply(window.cols, names(window.cols), sum)
  raw.cols<-unlist(raw.cols)
  raw.cols<-tapply(raw.cols, names(raw.cols), sum)
  return(list(window.cols, raw.cols))
}

collocateCollect<-function(collocate.names, source.folder, output.folder){
  ptm<-proc.time()
  
  all.files.collocates<-lapply(collocate.names, function(x) list.files(source.folder, full.name=T, recursive=T, pattern=x))
  col.nums<-unlist(lapply(all.files.collocates, function(x) length(x)))
  zero.cols<-which(col.nums<1)
  if(length(zero.cols)>0){
    collocate.names<-collocate.names[-zero.cols]
    all.files.collocates<-all.files.collocates[-zero.cols]
  }
  #distribute folders to the cluster
  all.tables<-lapply(all.files.collocates, function(x) lapply(x, function(y) read.csv(y, header=T, stringsAsFactors=F)))
  bound.tables<-lapply(all.tables, function(x) do.call("rbind", x))
  summed.lists<-lapply(bound.tables, function(x) tableToList(x))
  summed.lists<-lapply(summed.lists, function(x) lapply(x, function(y) tapply(y, names(y), sum)))
  all.terms<-lapply(summed.lists, function(x) names(x[[1]]))
  all.windows<-lapply(summed.lists, function(x) x[[1]])
  all.raw<-lapply(summed.lists, function(x) x[[2]])
  composite.tables<-mapply(function(x,y,z) data.frame(x,y,z), all.terms, all.windows, all.raw, SIMPLIFY=F)
  all.dims<-unlist(lapply(composite.tables, function(x) dim(x)))
  print(all.dims)
  for(i in 1:length(composite.tables)){
    colnames(composite.tables[[i]])<-c("Term", "WindowCols", "RawCols")
    output.filename<-paste(output.folder, "/AllCollocatesOf_", collocate.names[i], ".csv", sep="")
    write.csv(composite.tables[[i]], output.filename)
  }
}

##############################################
#code for combining two collocate result files (adding local data to remotely collected data)
vectorName<-function(vect.to.name, vect.names){
  names(vect.to.name)<-vect.names
  return(vect.to.name)
}

collocateTableCombine<-function(collocate.table.list){
  all.terms<-lapply(collocate.table.list, function(x) x$Term)
  all.window.cols<-lapply(collocate.table.list, function(x) x$WindowCols)
  all.window.cols<-mapply(function(x,y) vectorName(x,y), all.window.cols, all.terms)
  all.raw.cols<-lapply(collocate.table.list, function(x) x$RawCols)
  all.raw.cols<-mapply(function(x,y) vectorName(x,y), all.raw.cols, all.terms)
  all.window.cols<-unlist(all.window.cols)
  all.window.cols<-tapply(all.window.cols, names(all.window.cols), sum)
  all.raw.cols<-unlist(all.raw.cols)
  all.raw.cols<-tapply(all.raw.cols, names(all.raw.cols), sum)
  all.raw.cols<-all.raw.cols[order(names(all.raw.cols))]
  all.window.cols<-all.window.cols[order(names(all.window.cols))]
  final.terms<-names(all.window.cols)
  combined.table<-data.frame(final.terms, all.window.cols, all.raw.cols, stringsAsFactors=F)
  colnames(combined.table)<-c("Term", "WindowCols", "RawCols")
  return(combined.table)
}

combineCollocateTableFiles<-function(file1, file2, output.file){
  orig.table<-read.csv(file1, header=T, stringsAsFactors=F, row.names=1)
  add.table<-read.csv(file2, header=T, stringsAsFactors=F, row.names=1)
  composite.table<-collocateTableCombine(list(orig.table, add.table))
  write.csv(composite.table, output.file, row.names=F)
}


##############################################
#code for calculating the pmi/npmi and MDC/fishers exact test of collocates for a single target
calculatePMI<-function(col.window.freq, word.freq, target, n.windows){
  target.prob<-word.freq[which(names(word.freq) %in% target)]
  n.words<-sum(word.freq)
  target.prob<-(sum(target.prob))/n.words
  all.probs<-word.freq/n.words
  col.probs<-col.window.freq/n.windows
  individual.col.probs<-all.probs[which(names(word.freq) %in% names(col.window.freq))]
  col.probs<-col.probs[which(names(col.probs) %in% names(individual.col.probs))]
  individual.col.probs<-individual.col.probs[order(names(individual.col.probs))]
  col.probs<-col.probs[order(names(col.probs))]
  all.pmi<-individual.col.probs*target.prob
  all.pmi<-col.probs/all.pmi
  all.pmi<-log(all.pmi)
  entropy<-log(col.probs)
  entropy<-entropy*-1
  all.npmi<-all.pmi/entropy
  collocate.terms<-names(all.pmi)
  pmi.table<-data.frame(collocate.terms, all.pmi, all.npmi, stringsAsFactors=F)
  colnames(pmi.table)<-c("PMI_Term", "PMI", "NPMI")
  pmi.table<-pmi.table[order(pmi.table$PMI_Term),]
  return(pmi.table)
}

mdCollocates<-function(col.raw.freq, word.freq){
  word.freq<-word.freq/sum(word.freq)
  word.freq<-word.freq[which(names(word.freq) %in% names(col.raw.freq))]
  col.raw.freq<-col.raw.freq[which(names(col.raw.freq) %in% names(word.freq))]
  word.freq<-word.freq[order(names(word.freq))]
  col.raw.freq<-col.raw.freq[order(names(col.raw.freq))]
  total.cols<-sum(col.raw.freq)
  obs.collocates<-col.raw.freq
  exp.collocates<-round((word.freq*total.cols),0)
  all.obs.exp<-obs.collocates/exp.collocates
  contingency.tables<-mapply(function(x,y) matrix(c(x,(total.cols-x), y, (total.cols-y)), ncol=2, byrow=T), obs.collocates, exp.collocates, SIMPLIFY=F)
  fisher.results<-lapply(contingency.tables, function(x) fisher.test(x))
  fisher.pvalues<-lapply(fisher.results, function(x) x$p.value)
  mdc.table<-data.frame(names(col.raw.freq), col.raw.freq, col.raw.freq/total.cols, all.obs.exp, unlist(fisher.pvalues))
  colnames(mdc.table)<-c("Term", "NObs", "FreqObs", "Obs/Exp", "pValue")
  mdc.table<-mdc.table[order(mdc.table$Term),]
  return(mdc.table)
}

calculateColStats<-function(target, collocate.table, year.freq.table, year.summary.stats, alpha, min.obs, stop.words){
  print(target)
  #temporary code to fix negative values
  collocate.table<-collocate.table[which(collocate.table$RawCols>0),]
  #end temporary code
  all.word.freq<-colSums(year.freq.table)
  n.windows<-sum(year.summary.stats$Num_21Word_Windows)
  window.cols<-collocate.table$WindowCols
  names(window.cols)<-collocate.table$Term
  print("Finding PMI")
  pmi.table<-calculatePMI(window.cols, all.word.freq, target, n.windows)
  raw.cols<-collocate.table$RawCols
  names(raw.cols)<-collocate.table$Term
  print("Finding MDC")
  mdc.table<-mdCollocates(raw.cols, all.word.freq)
  final.table<-data.frame(mdc.table, pmi.table, stringsAsFactors = F)
  final.table<-final.table[order(final.table$NPMI, decreasing=T),]
  final.table<-final.table[which(final.table$pValue<alpha),]
  final.table<-final.table[which(final.table$NObs>=min.obs),]
  if(!is.null(stop.words)){
    sw<-rep(F, nrow(final.table))
    sw[which(final.table$Term %in% stop.words)]<-T
    final.table$Stopword<-sw
  }
  return(final.table)
}

allCollocateResults<-function(source.folder, target.list, output.folder, freq.table.filename, stat.table.filename, alpha=0.05, min.obs=5, sw=NULL){
  #freq.table<-read.csv(paste(source.folder, freq.table, sep="/"), header=T, stringsAsFactors=F, row.names=1)
  #summary.table<-read.csv(paste(source.folder, stat.table, sep="/"), header=T, stringsAsFactors = F)
  freq.table<-freq.table.filename
  summary.table<-stat.table.filename
  collocate.table.files<-unlist(lapply(target.list, function(x) list.files(source.folder, full.names=T, pattern=x)))
  collocate.tables<-lapply(collocate.table.files, function(x) read.csv(x, header=T, stringsAsFactors = F))
  #print(unlist(collocate.table.files))
  #print(unlist(lapply(collocate.tables, function(x) dim(x))))
  print(length(collocate.tables))
  all.stat.tables<-mapply(function(x,y) calculateColStats(x, y, freq.table, summary.table, alpha, min.obs, sw), target.list, collocate.tables, SIMPLIFY = F)
  target.names<-unlist(lapply(target.list, function(x) x[1]))
  output.filenames<-lapply(target.names, function(x) paste(output.folder, "/CollocateStatsOn_", x, ".csv", sep=""))
  mapply(function(x,y) write.csv(x,y,row.names=F), all.stat.tables, output.filenames, SIMPLIFY=F)
}

###############################################
#code for smoothing frequencies over time with a rolling average
#code iterates smoothing for n frequencies usng a vector for smoothing.factor

smoothFreqOverTime<-function(freq.vector, smoothing.factor){
  if(length(freq.vector)<=smoothing.factor){
    print(paste("Unable to smooth at:", as.character(smoothing.factor), sep=" "))
  } else {
    smoothing.half<-ceiling(((smoothing.factor-1)/2))
    freq.index<-seq(1, length(freq.vector), by=1)
    smoothing.indicies<-lapply(freq.index, function(x) seq((x-smoothing.half), (x+smoothing.half), by=1))
    smoothing.indicies<-lapply(smoothing.indicies, function(x) x[which(x %in% freq.index)])
    smoothing.vector<-lapply(smoothing.indicies, function(x) freq.vector[x])
    smoothed.vector<-lapply(smoothing.vector, function(x) mean(x))
    smoothed.vector<-unlist(smoothed.vector)
    return(smoothed.vector)
  }
}

#here, making smoothing.factor a vector will iterate over the smothing.factor, returning a new column for each value
smoothTableIterate<-function(freq.table, smoothing.factor, variable.name, prob.col=2){
  smoothed.probs<-lapply(smoothing.factor, function(x) smoothFreqOverTime(freq.table[,prob.col],x))
  smoothed.df<-do.call("cbind", smoothed.probs)
  column.labels<-unlist(lapply(smoothing.factor, function(x) paste(variable.name, "smoothed_avg", as.character(x), sep="_")))
  colnames(smoothed.df)<-column.labels
  colnames(freq.table)<-c(paste(variable.name, "raw_freq", sep="_"), paste(variable.name, "raw_prob", sep="_"))
  final.table<-data.frame(freq.table, smoothed.df, stringsAsFactors = F)
  return(final.table)
}

makeSmoothFrequencyTables<-function(target.list, all.freq, output.folder, smoothing.factor){
  year.freq<-rowSums(all.freq)
  all.tables<-lapply(target.list, function(x) all.freq[,which(colnames(all.freq) %in% x)])
  all.tables<-lapply(all.tables, function(x) if(!is.null(ncol(x))){rowSums(x)}else{x})
  all.probs<-lapply(all.tables, function(x) x/year.freq)
  all.probs<-lapply(all.probs, function(x) x*1000000)
  all.composite.tables<-mapply(function(x,y) data.frame(x,y,stringsAsFactors=F), all.tables, all.probs, SIMPLIFY=F)
  target.names<-lapply(target.list, function(x) x[[1]])
  column.labels<-lapply(target.names, function(x) c(paste(x, "RawFrequency", sep="_"), paste(x, "Frequency_per_1M", sep="_")))
  for(i in 1:length(column.labels)){
    colnames(all.composite.tables[[i]])<-column.labels[[i]]
    #print(dim(all.composite.tables[i]))
  }
  smoothed.tables<-mapply(function(x,y) smoothTableIterate(x, smoothing.factor, y), all.composite.tables, target.names, SIMPLIFY=F)
  smoothed.table<-do.call("cbind", smoothed.tables)
  Year<-seq(1660, 1860, by=1)
  smoothed.table<-data.frame(Year, smoothed.table, stringsAsFactors = F)
  write.csv(smoothed.table, paste(output.folder, "Top5PercSublime_SmoothedFrequencyTable.csv", sep="/"), row.names=F)
  source("/Users/malgeehe/dropbox/text mining/matrixTransform.r")
  melted.table<-melt(smoothed.table)
  label.column<-as.character(melted.table[,3])
  split.labels<-lapply(label.column, function(x) unlist(strsplit(x, "_")))
  Term<-unlist(lapply(split.labels, function(x) x[1]))
  Stat<-lapply(split.labels, function(x) x[2:length(x)])
  Stat<-unlist(lapply(Stat, function(x) paste(x, collapse="_")))
  final.table<-data.frame(melted.table[,1:2], Term, Stat, stringsAsFactors=F)
  colnames(final.table)<-c("Year", "Value", "Term", "Stat")
  write.csv(final.table, paste(output.folder, "Top5PercSublime_MeltedFrequencyTable.csv", sep="/"), row.names=F)
}

######################################################
#code to gather all intances of a group of words from year/corpus frequency tables
#preparation for clustering terms
#depends on importFreqTable above

subsetFreqTable<-function(filename, word.list){
  full.freq.table<-importFreqTable(filename)
  full.freq.table<-full.freq.table[,which(colnames(full.freq.table) %in% word.list)]
  test.matrix<-ncol(full.freq.table)
  if(length(test.matrix)>0){
    scaled.freq.table<-full.freq.table/rowSums(full.freq.table)
  } else {
    scaled.freq.table<-full.freq.table/sum(full.freq.table)
  }
  
  return(scaled.freq.table)
}

buildYearTable<-function(year.folder, word.list){
  all.table.filenames<-list.files(year.folder, pattern="frequencytable.csv", full.names=T)
  all.corpus.tables<-lapply(all.table.filenames, function(x) subsetFreqTable(x, word.list))
  all.corpus.tables<-do.call("rbind", all.corpus.tables)
  print(dim(all.corpus.tables))
  curr.cols<-colnames(all.corpus.tables)
  curr.year<-unlist(strsplit(year.folder, "/"))
  curr.year<-curr.year[length(curr.year)]
  curr.year<-as.numeric(curr.year)
  print(curr.year)
  year.col<-rep(curr.year, nrow(all.corpus.tables))
  all.corpus.tables<-cbind(all.corpus.tables, year.col)
  colnames(all.corpus.tables)<-c(curr.cols, "Date")
  print(dim(all.corpus.tables))
  return(all.corpus.tables)
}

buildSubsetFreqTables<-function(root.input, output.folder, word.list){
  all.year.folders<-list.dirs(root.input, recursive=F)
  print(all.year.folders)
  library(parallel)
  size<-detectCores()
  np<- if (size>1) size-1 else 1
  print(np)
  proc.clust<-makeCluster(np, type="FORK")
  all.year.tables<-parLapply(proc.clust, all.year.folders, function(x) buildYearTable(x, word.list))
  #all.year.tables<-lapply(all.year.folders, function(x) buildYearTable(x, word.list))
  stopCluster(proc.clust)
  final.table<-do.call("rbind", all.year.tables)
  write.csv(final.table, paste(output.folder, "NewSublimeCollocateFreqTable_Scaled.csv", sep="/"), row.names=F)
  return(final.table)
}

#######################################
#the following code finds the pmi window counts of all targets within a given horizon
#it then trims the resulting code based on the list of targets to find all target collocates of the targets
#(it creates a symmetric distance table of windowed counts per year of targets to targets)
#note that it uses the window calculating code from editWindow above

gatherTargetCollocates<-function(target, split.text, horizon, target.list){
  target.hits<-which(split.text %in% target)
  if(length(target.hits)>0){
    hit.counts<-split.text[target.hits]
    hit.adjust<-table(hit.counts)
    hit.adjust<-hit.adjust*-1
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
    all.window.cols<-lapply(all.cols, function(x) editWindow(x, target, horizon))
    all.window.cols<-unlist(all.window.cols)
    all.window.cols<-tapply(all.window.cols, names(all.window.cols), sum)
    blank.index<-which(names(all.window.cols)=="")
    if(length(blank.index)>0){
      all.window.cols<-all.window.cols[-blank.index]
    }
    target.matches<-which(names(all.window.cols) %in% target.list)
    if(length(target.matches)>0){
      all.window.cols<-all.window.cols[target.matches]
    } else {
      return(NA)
    }
  }
  else{
    return(NA)
  }
}

collocateListCounts<-function(text.fn, dictionary, target.list, horizon){
  test.scan<-tryCatch(raw.text<-scan(text.fn, what='character', sep="\n", quiet=T), error=function(e){return("NotFound")})
  if(length(test.scan)>0){
    if(test.scan=="NotFound"){
      return(NA)
    } else {
      raw.text<-paste(raw.text, collapse=" ")
      clean.text<-cleanText(raw.text)
      remove(raw.text)
      if(!is.null(dictionary)){
        clean.text<-clean.text[which(clean.text %in% dictionary)]
      }
      collocate.tables<-lapply(target.list, function(x) gatherTargetCollocates(x, clean.text, horizon, unlist(target.list)))
      remove(test.scan)
      gc()
      return(collocate.tables)
    }
  }
}

findTargetCollocatesYear<-function(sub.meta, dictionary, output.folder.root, filename.col, target.list, horizon, base.collocate.table){
  year<-unique(sub.meta$year_fixed)
  
  #make the new folder
  output.subfolder<-paste(output.folder.root, as.character(year), sep="/")
  
  all.paths<-sub.meta[,filename.col]
  collocate.tables<-lapply(all.paths, function(x) collocateListCounts(x, dictionary, target.list, horizon))
  
  for(i in 1:length(base.collocate.table)){
    all.year.hits<-unlist(lapply(collocate.tables, function(x) x[i]))
    curr.collocate.list<-c(base.collocate.table[[i]], all.year.hits)
    curr.collocate.list<-tapply(curr.collocate.list, names(curr.collocate.list), sum)
    base.collocate.table[[i]]<-curr.collocate.list
  }
  full.table<-do.call("rbind", base.collocate.table)
  rownames(full.table)<-target.list
  full.table<-full.table[order(rownames(full.table)),]
  full.table<-full.table[,order(colnames(full.table))]
  bad.cols<-which(!colnames(full.table) %in% target.list)
  if(length(bad.cols)>0){
    full.table<-full.table[,-bad.cols]
  }
  write.csv(full.table, paste(output.subfolder, "TargetWindowDistanceTable_Horizon10.csv", sep="/"))
  remove(collocate.tables)
  remove(full.table)
  remove(all.year.hits)
  remove(curr.collcoate.list)
  gc()
}


#function to build indiviudal collocate tables per corpus per year
targetDistanceTables<-function(full.meta.table, dictionary, output.folder.top, target.list, horizon, filename.col="fn"){
  ptm<-proc.time()
  #library(Rmpi)
  library(parallel)
  
  #extract the paths from the metadata table
  #all.paths<-full.meta.table[,filename.col]
  
  #start cluster
  #size<-mpi.universe.size()
  #np <- if (size >1 ) size - 1 else 1
  #cluster<-makeCluster(np, type='MPI', outfile='')
  size<-detectCores()
  np<- if (size>1) size-1 else 1
  print(np)
  proc.clust<-makeCluster(np, type="FORK")
  
  #spread variables (dictionary, filenames, textCounts function) and return list of word counts per text with load balanced apply
  #clusterExport(cluster, list("dictionary", "textCounts"))
  #all.tables<-parLapply(cl=proc.clust, all.paths, function(x) textCounts(x, dictionary))
  
  base.collocate.table<-list()
  for(i in 1:length(target.list)){
    curr.target<-rep(0, length(target.list))
    names(curr.target)<-target.list
    base.collocate.table<-c(base.collocate.table, list(curr.target))
  }
  names(base.collocate.table)<-target.list
  
  unique.years<-unique(full.meta.table$year_fixed)
  print(unique.years)
  completed.years<-completedCollocateDistanceFiles(output.folder.top, horizon)
  year.match<-which(unique.years %in% completed.years)
  if(length(year.match)>0){
    unique.years<-unique.years[-year.match]
  }
  print(unique.years)
  
  if(length(unique.years)>0){
    sub.tables<-lapply(unique.years, function(x) full.meta.table[which(full.meta.table$year_fixed==x),])
    parLapply(proc.clust, sub.tables, function(x) findTargetCollocatesYear(x, dictionary, output.folder.top, filename.col, target.list, horizon, base.collocate.table))
    #lapply(sub.tables, function(x) writeCollocateTablesYear(x, dictionary, output.folder.top, curr.corpus, filename.col, target.list, horizon))
    #mapply(function(x,y) writeFreqTableYear(x, dictionary, output.folder, curr.corpus, proc.clust, filename.col, y), sub.tables, unique.years, SIMPLIFY=F)
  }
  stopCluster(proc.clust)
  print(proc.time()-ptm)
}


##############################################
#code to find files that are done for exclusion
completedCollocateDistanceFiles<-function(folder.top, horizon){
  #print(curr.corpus)
  horizon.filename<-paste("TargetWindowDistanceTable_Horizon", as.character(horizon), sep="")
  all.files<-list.files(folder.top, recursive=T, full.names=T, pattern=horizon.filename)
  if(length(all.files)>0){
    all.files.split<-lapply(all.files, function(x) unlist(strsplit(x, "/")))
    corpus.years<-unlist(lapply(all.files.split, function(x) x[(length(x)-1)]))
    corpus.years<-unique(corpus.years)
    return(corpus.years)
  } else {
    return(NA)
  }
}

#####################################
#code to calculate the pmi distance table per year, or per groups of years, according to the distance tables
#given a distance table, the functions need to:
#1) scale the values of collocates row/column by dividing by the number of windows per year
#2) create an equivalent distance table of the prob.row * prob.col the same dimension as the distance table
#3) take the log of the table
#4) If needed, calculate entropy and scale the table for npmi
#variables needed: 
#1. root of folder system with year distance tables
#2. table of scaled word frequencies per year
#3. table of summary stats per year (including total words and total windows)

calculatePMIDistTable<-function(dist.table, scaled.year.freqs, year.summary.stats){
  sum.stats<-year.summary.stats[which(names(year.summary.stats)=="Num_21Word_Windows")]
  scaled.dist<-dist.table/year.summary.stats[which(names(year.summary.stats)=="Num_21Word_Windows")]
  scaled.year.freqs<-scaled.year.freqs[which(names(scaled.year.freqs) %in% colnames(dist.table))]
  term.one<-unlist(rep(scaled.year.freqs, nrow(dist.table)))
  term.two<-term.one[order(names(term.one))]
  probs.m<-term.one*term.two
  prob.dists<-matrix(probs.m, ncol=ncol(dist.table), byrow=T)
  #return(prob.dists)
  pmi<-scaled.dist/prob.dists
  pmi<-log(pmi)
  pmi[pmi==-Inf]<-0
  pmi[is.na(pmi)]<-0
  entropy<-log(scaled.dist)
  entropy<--1*entropy
  npmi<-pmi/entropy
  npmi[is.na(npmi)]<-0
  pmi.list<-list(pmi, npmi)
  return(pmi.list)
}


#function to calculate the mean pmi and npmi per year based on windowed coocurrence tables, also allows subsetting a wordlist
#This function should be depreciated: both pmi and npmi seem highly correlated to the total words per year - maybe a version that 
#only takes the mean of positive values
meanMPIDecade<-function(pmi.table.root, freq.table, stat.table, word.list=NULL, output.folder){
  all.table.filenames<-list.files(pmi.table.root, full.names=T)
  all.table.names<-list.files(pmi.table.root)
  date.list<-NULL
  pmi.avg<-NULL
  npmi.avg<-NULL
  for(i in 1:length(all.table.filenames)){
    curr.dist.table<-read.csv(file=all.table.filenames[i], header=T, stringsAsFactors=F, row.names=1)
    curr.dist.table<-as.matrix(curr.dist.table)
    curr.freq<-freq.table[i,]
    names(curr.freq)<-colnames(freq.table)
    curr.stats<-stat.table[i,]
    names(curr.stats)<-colnames(stat.table)
    curr.date<-unlist(strsplit(all.table.names[i], "_"))
    curr.date<-curr.date[1]
    date.list<-c(date.list, curr.date)
    if(!is.null(word.list)){
      match.index<-which(rownames(curr.dist.table) %in% word.list)
      curr.dist.table<-curr.dist.table[match.index, match.index]
    }
    curr.pmi.list<-calculatePMIDistTable(curr.dist.table, curr.freq, curr.stats)
    curr.pmi.mean<-mean(curr.pmi.list[[1]])
    curr.npmi.mean<-mean(curr.pmi.list[[2]])
    pmi.avg<-c(pmi.avg, curr.pmi.mean)
    npmi.avg<-c(npmi.avg, curr.npmi.mean)
  }
  pmi.avg.table<-data.frame(date.list, pmi.avg, npmi.avg)
  colnames(pmi.avg.table)<-c("Year", "Mean_PMI", "Mean_NPMI")
  write.csv(pmi.avg.table, file=paste(output.folder, "MeanPMI_Year_Top5Sublime.csv", sep="/"), row.names=F)
  return(pmi.avg.table)
}

#misc code to add remote and local target distance tables together and put them in a new folder
distTableAdd<-function(remote.folder.root, local.folder.root, output.folder){
  all.subfolders<-list.dirs(remote.folder.root, full.names=F, recursive=F)
  remote.files<-lapply(all.subfolders, function(x) paste(remote.folder.root, x, "TargetWindowDistanceTable_Horizon10.csv", sep="/"))
  local.files<-lapply(all.subfolders, function(x) paste(local.folder.root, x, "TargetWindowDistanceTable_Horizon10.csv", sep="/"))
  print(remote.files[1:5])
  remote.tables<-lapply(remote.files, function(x) read.csv(x, header=T, stringsAsFactors=F, row.names=1))
  local.tables<-lapply(local.files, function(x) read.csv(x, header=T, stringsAsFactors=F, row.names=1))
  summed.tables<-mapply(function(x,y) x+y, remote.tables, local.tables, SIMPLIFY = F)
  output.names<-paste(all.subfolders, "TargetDistanceTable_Horizon10.csv", sep="_")
  output.names<-paste(output.folder, output.names, sep="/")
  print(length(output.names))
  print(output.names[1:5])
  mapply(function(x,y) write.csv(x,y, row.names=T), summed.tables, output.names)
}

#code to sum a number of dist tables from files and find the pmi/npmi of them
#to add: an aspect of the function that subsets the dates based on the filenames after being passed a vector [achieved with wrapper]
#note: root.dir indicates whether pmi.table.root is a folder, or a list of filenames (added to work with date wrapper)
sumDistTablesPMI<-function(pmi.table.root, freq.table, stat.table, word.list=NULL, root.dir=T, frequency.names=NULL){
  if(root.dir){
    all.table.filenames<-list.files(pmi.table.root, full.names=T)
    all.table.names<-list.files(pmi.table.root)
  } else {
    all.table.filenames<-pmi.table.root
  }
  all.tables<-lapply(all.table.filenames, function(x) read.csv(x, header=T, stringsAsFactors=F, row.names=1))
  all.tables<-lapply(all.tables, function(x) as.matrix(x))
  summed.table<-Reduce('+', all.tables)
  if(!is.null(dim(freq.table))){
    freqs<-colSums(freq.table)
    names(freqs)<-colnames(freq.table)
  } else {
    freqs<-freq.table
    names(freqs)<-frequency.names
  }
  freqs<-freqs/sum(freqs)
  if(!is.null(dim(freq.table))){
    summed.stats<-colSums(stat.table)
  } else {
    summed.stats<-stat.table
    summed.stats<-unlist(stat.table)
    names(summed.stats)<-c("Num_Texts", "Num_21Word_Windows", "TotalWords", "Year")
  }
  names(summed.stats)<-colnames(stat.table)
  if(!is.null(word.list)){
    match.index<-which(rownames(summed.table) %in% word.list)
    summed.table<-summed.table[match.index, match.index]
  }
  full.pmi.list<-calculatePMIDistTable(summed.table, freqs, summed.stats)
  return(full.pmi.list)
}

#################################################################
#code to retrieve all horizon segments for a target and test word
gatherKwics<-function(target, text.fn, horizon, test.words){
  test.scan<-tryCatch(raw.text<-scan(text.fn, what='character', sep="\n", quiet=T), error=function(e){return("NotFound")})
  if(length(test.scan)>0){
    if(test.scan=="NotFound"){
      return(NA)
    } else {
      raw.text<-paste(raw.text, collapse=" ")
      split.text<-cleanText(raw.text)
      remove(raw.text)
      target.hits<-which(split.text %in% target)
      testword.hits<-which(split.text %in% test.words)
      if(length(target.hits)>0){
        if(length(testword.hits)>0){
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
          test.word.found<-unlist(lapply(all.cols, function(x) length(which(x %in% test.words))))
          kwic.hits<-which(test.word.found>0)
          if(length(kwic.hits)>0){
            kwics<-all.cols[kwic.hits]
            kwics<-unlist(lapply(kwics, function(x) paste(x, collapse=" ")))
            return(kwics)
          } else {
            return(NA)
          }
        }
      }
      else{
        return(NA)
      }
    }
  }
}

#function to gather specific kwics across the entire corpus
targetKwics<-function(full.meta.table, output.folder, output.filename, target, test.words, horizon, filename.col="fn"){
  ptm<-proc.time()
  #library(Rmpi)
  library(parallel)
  
  size<-detectCores()
  np<- if (size>1) size-1 else 1
  print(np)
  proc.clust<-makeCluster(np, type="FORK")
  
  all.filenames<-full.meta.table[,filename.col]
  all.kwicks<-parLapply(proc.clust, all.filenames, function(x) gatherKwics(target, x, horizon, test.words))
  #all.kwicks<-lapply(all.filenames, function(x) gatherKwics(target, x, horizon, test.words))
  all.kwicks<-unlist(all.kwicks)
  bad.kwicks<-which(is.na(all.kwicks))
  if(length(bad.kwicks)>0){
    all.kwicks<-all.kwicks[-bad.kwicks]
  }
  write.csv(all.kwicks, file=paste(output.folder, output.filename, sep="/"))
  
  stopCluster(proc.clust)
  print(proc.time()-ptm)
}

############################################################
#code to take a folder of distance tables (created by targetDistanceTables above), create an mpi distance table out
#of date ranges, and create a dense vector representation with svd
#for every date range, it outputs a 10 column vector model, and a plot table with tsne coords
#can also take group assignments to include in the plot table

#creates dense vector representation of column.cut length from a symmetric pmi distance table
createSVDVector<-function(pmi.dist.table, column.cut=10){
  svd.model<-svd(pmi.dist.table)
  svd.model<-svd.model$u
  svd.model<-svd.model[,1:column.cut]
  rownames(svd.model)<-rownames(pmi.dist.table)
  return(svd.model)
}

#finds the mean distance from a target term for the terms from each group
#function excludes the target term from distance calculations (and drops it from its group)
groupTargetDistance<-function(pmi.vectors, group.assignments, unique.groups, target.term){
  group.terms<-lapply(unique.groups, function(x) names(group.assignments[which(group.assignments==x)]))
  target.distance.vector<-getTopTerms(pmi.vectors, target.term, n.return=nrow(pmi.vectors))
  target.distance.vector<-target.distance.vector[-1]
  all.group.distances<-lapply(group.terms, function(x) target.distance.vector[which(names(target.distance.vector) %in% x)])
  target.group.distances<-unlist(lapply(all.group.distances, function(x) mean(x)))
  return(target.group.distances)
}

#functions try to approximate group coherence by taking the mean of the distance from each word in a group to every other word
meanGroupDistances<-function(pmi.vectors, group.terms){
  all.distances<-lapply(group.terms, function(x) getTopTerms(pmi.vectors, x, n.return=nrow(pmi.vectors)))
  all.distances<-lapply(all.distances, function(x) x[-1])
  all.distances<-lapply(all.distances, function(x) x[which(names(x) %in% group.terms)])
  all.distance.means<-unlist(lapply(all.distances, function(x) mean(x)))
  group.mean.distance<-mean(all.distance.means)
  return(group.mean.distance)
}

groupCoherence<-function(pmi.vectors, group.assignments, unique.groups){
  group.terms<-lapply(unique.groups, function(x) names(group.assignments[which(group.assignments==x)]))
  all.group.means<-lapply(group.terms, function(x) meanGroupDistances(pmi.vectors, x))
  group.mean.distances<-unlist(all.group.means)
  return(group.mean.distances)
}

#function to plot svd vector models as tsne plots
plotVectorSpace<-function(svd.model, date.range.name, output.folder, group.assignments){
  library(Rtsne)
  library(ggplot2)
  output.filename<-paste("Tsne_model_of_", date.range.name, ".pdf", sep="")
  output.filename<-paste(output.folder, output.filename, sep="/")
  print(output.filename)
  tsne.model<-Rtsne(svd.model, perplexity = 40, max_iter=10000, check_duplicates = F)
  tsne.coords<-tsne.model$Y
  terms<-names(group.assignments)
  plot.table<-data.frame(tsne.coords, terms, as.character(group.assignments))
  colnames(plot.table)<-c("X", "Y", "Term", "Group")
  plot.table.filename<-paste("Tsne_Plot_Table_of_", date.range.name, ".csv", sep="")
  plot.table.filename<-paste(output.folder, plot.table.filename, sep="/")
  write.csv(plot.table, plot.table.filename, row.names=F)
  output.plot<-ggplot(plot.table, aes(x=X, y=Y, color=Group, label=Term))+geom_point(size=.4)+geom_text(size=2)
  pdf(output.filename, width=10, height=10)
  print(output.plot)
  dev.off()
}


#Primary wrapper function
#NOTE - need raw freq table and yearly stat table to make the pmi calculations
#i.e. NewSublimeCompositeFrequencyTable.csv, and summarystats_fixed.csv
#also TargetDistanceTables have the full 527 new sublime wordlist so this needs to be filtered to the top 5
#Note - depends on order of files in table.folder.root, and rows in freq.table and stat.table being the same
#output tsne will output a tsne plot using the group assignments for every date segment
#group assignments is the output of an hclust (or something that looks like it) - a series of numbers representing groups with the individual terms as names
dateSegmentVectorModelGroups<-function(table.folder.root, freq.table, stat.table, year.seq.length, wordlist, output.folder, group.assignments, output.tsne=F, target.word="sublime", glove.functions="/users/malgeehe/dropbox/text mining/WordVectors/GloveFunctions.R"){
  source(glove.functions)
  date.segment.starts<-seq(min(stat.table$Year), max(stat.table$Year), by=year.seq.length)
  date.segments<-lapply(date.segment.starts, function(x) seq(x, ((x+year.seq.length)-1), by=1))
  date.segment.indicies<-lapply(date.segments, function(x) which(stat.table$Year %in% x))
  segment.lengths<-unlist(lapply(date.segment.indicies, function(x) length(x)))
  segment.names<-unlist(lapply(date.segments, function(x) paste(as.character(x[1]), as.character(x[length(x)]), sep="_")))
  print(segment.names)
  all.filenames<-list.files(table.folder.root, full.names=T)
  segmented.filenames<-lapply(date.segment.indicies, function(x) all.filenames[x])
  segmented.frequencies<-lapply(date.segment.indicies, function(x) freq.table[x,])
  segmented.stats<-lapply(date.segment.indicies, function(x) stat.table[x,])
  all.pmi.distance.tables<-mapply(function(x,y,z,a) sumDistTablesPMI(x, y, z, word.list=wordlist, root.dir=F, frequency.names=colnames(freq.table)), segmented.filenames, segmented.frequencies, segmented.stats, SIMPLIFY=F)
  pmi.distances<-lapply(all.pmi.distance.tables, function(x) x[[1]])
  all.svd.models<-lapply(pmi.distances, function(x) createSVDVector(x))
  unique.groups<-unique(group.assignments)
  all.mean.group.distances<-lapply(all.svd.models, function(x) groupTargetDistance(x, group.assignments, unique.groups, target.word))
  all.coherence<-lapply(all.svd.models, function(x) groupCoherence(x, group.assignments, unique.groups))
  all.mean.group.distances<-unlist(all.mean.group.distances)
  all.coherence<-unlist(all.coherence)
  group.column<-rep(unique.groups, length(all.svd.models))
  year.segment.names<-unlist(lapply(segment.names, function(x) rep(x, length(unique.groups))))
  segment.table<-data.frame(year.segment.names, group.column, all.mean.group.distances, all.coherence)
  colnames(segment.table)<-c("Date_Range", "Group", "Distance_from_Sublime", "Group_Coherence")
  if(output.tsne){
    mapply(function(x,y) plotVectorSpace(x,y,output.folder, group.assignments), all.svd.models, segment.names)
  }
  write.csv(segment.table, file=paste(output.folder, "Group_Distance_and_Coherence.csv", sep="/"), row.names=F)
  return(segment.table)
}


#####################################
#Functions to explore correlations between different groups of words
#these depend on both a regular frequencdy table, and a melted frequency table - using melt function in matrix.transform on scaled frequency table made above
correlationTable<-function(scaled.freq.table){
  cor.table<-cor(scaled.freq.table)
  return(cor.table)
}



#get the top correlated terms of a target word
topCorrelatedTerms<-function(cor.table, target.term, threshold=.75){
  target.cors<-cor.table[,which(colnames(cor.table)==target.term)]
  names(target.cors)<-rownames(cor.table)
  target.cors<-sort(target.cors, decreasing=T)
  target.cors<-target.cors[which(target.cors>threshold)]
  return(target.cors)
}

#get the top correlated terms from a group of words
clusterCors<-function(cor.table, target.list, threshold=.75){
  all.top.cors<-lapply(target.list, function(x) topCorrelatedTerms(cor.table, x, threshold))
  all.top.cors<-unlist(all.top.cors)
  all.top.cors<-names(all.top.cors[-which(duplicated(names(all.top.cors)))])
  return(all.top.cors)
}

#find divergent correlations for a target term before and after a specific date
#table dates is a vector dates corresponding to the rows of the frequency table
divergentCors<-function(scaled.freq.table, date.split, table.dates, target.term){
  pre.freqs<-scaled.freq.table[which(table.dates<=date.split),]
  post.freqs<-scaled.freq.table[which(table.dates>date.split),]
  pre.cor<-correlationTable(pre.freqs)
  post.cor<-correlationTable(post.freqs)
  target.pre<-pre.cor[,which(colnames(pre.cor)==target.term)]
  target.post<-post.cor[,which(colnames(post.cor)==target.term)]
  divergence<-target.pre-target.post
  names(divergence)<-rownames(post.cor)
  divergence<-sort(divergence, decreasing=T)
  return(divergence)
}

fixInfPmi<-function(pmi.table){
  pmi.table[which(pmi.table==Inf)]<-100
  return(pmi.table)
}

#function takes a folder of year co-occurence tables (as functions above), creates a pmi distance table per year.seq.length, then a
#svd vector model for each year. 
#For each year function finds the n.top terms closest to target.term and creates a melted plot table to plot all words over time
vectorTargetDistanceOverTime<-function(table.folder.root, freq.table, stat.table, year.seq.length, wordlist, output.folder, target.term, n.top, glove.functions="/users/malgeehe/dropbox/text mining/WordVectors/GloveFunctions.R"){
  source(glove.functions)
  date.segment.starts<-seq(min(stat.table$Year), max(stat.table$Year), by=year.seq.length)
  date.segments<-lapply(date.segment.starts, function(x) seq(x, ((x+year.seq.length)-1), by=1))
  date.segment.indicies<-lapply(date.segments, function(x) which(stat.table$Year %in% x))
  segment.lengths<-unlist(lapply(date.segment.indicies, function(x) length(x)))
  segment.names<-unlist(lapply(date.segments, function(x) paste(as.character(x[1]), as.character(x[length(x)]), sep="_")))
  print(segment.names)
  all.filenames<-list.files(table.folder.root, full.names=T)
  segmented.filenames<-lapply(date.segment.indicies, function(x) all.filenames[x])
  segmented.frequencies<-lapply(date.segment.indicies, function(x) freq.table[x,])
  segmented.stats<-lapply(date.segment.indicies, function(x) stat.table[x,])
  all.pmi.distance.tables<-mapply(function(x,y,z,a) sumDistTablesPMI(x, y, z, word.list=wordlist, root.dir=F, frequency.names=colnames(freq.table)), segmented.filenames, segmented.frequencies, segmented.stats, SIMPLIFY=F)
  pmi.distances<-lapply(all.pmi.distance.tables, function(x) x[[1]])
  all.svd.models<-lapply(pmi.distances, function(x) createSVDVector(x))
  all.distances<-lapply(all.svd.models, function(x) getTopTerms(x, target.term, n.return=nrow(all.svd.models[[1]])))
  all.top.terms<-lapply(all.distances, function(x) x[2:(n.top+1)])
  all.top.terms<-unlist(all.top.terms)
  if(length(which(duplicated(all.top.terms)))>0){
    all.top.terms<-all.top.terms[-which(duplicated(all.top.terms))]
  }
  #top.terms<-sort(tapply(all.top.terms, names(all.top.terms), sum), decreasing=T)
  #all.top.terms<-top.terms[1:20]
  all.distances<-lapply(all.distances, function(x) x[which(names(x) %in% names(all.top.terms))])
  all.plot.tables<-mapply(function(x,y) data.frame(names(x), x, rep(y, length(x), stringsAsFactors=F)), all.distances, segment.names, SIMPLIFY=F)
  final.time.table<-do.call("rbind", all.plot.tables)
  colnames(final.time.table)<-c("Term", "Distance", "Year")
  return(final.time.table)
  output.filename<-paste("Distance_from_", target.term, "over_time.csv", sep="_")
  output.filename<-paste(output.folder, output.filename, sep="/")
  write.csv(final.time.table, output.filename, row.names=F)
  return(all.plot.tables)
}

#to write:
#function that takes a folder of distance tables, a freq table and stat table (as sumDisttables above) and a date range and
#a group assignment
#returns a plot table of tnse with coords and groups - should also allow the assignment of groups if requested


#to write:
#a function that takes a folder of distance tables, a freq table and stat table (as meanPMIDecade above) but creates a 
#dense vector representation of svd on pmi per time window, then calcuates a score for each window by finding the mean
#distance between all words 


#############################
#code to count all words in the corpus of frequency tables
#used to double check all numbers
readSumTable<-function(table.filename){
  curr.table<-importFreqTable(table.filename)
  curr.sum<-sum(curr.table)
  remove(curr.table)
  gc()
  return(curr.sum)
}


countCorpora<-function(year.folder, clust.obj){
  print(year.folder)
  all.files<-list.files(year.folder, full.names=T)
  freq.files<-all.files[grep("frequencytable.csv", all.files)]
  corpus.names<-lapply(freq.files, function(x) unlist(strsplit(x, "/")))
  corpus.names<-lapply(corpus.names, function(x) x[length(x)])
  corpus.names<-lapply(corpus.names, function(x) unlist(strsplit(x, "_")))
  corpus.names<-lapply(corpus.names, function(x) x[c(1,2)])
  corpus.names<-unlist(lapply(corpus.names, function(x) paste(x, collapse="_")))
  print(corpus.names)
  all.sums<-parLapply(clust.obj, freq.files, function(x) readSumTable(x))
  all.sums<-unlist(all.sums)
  names(all.sums)<-corpus.names
  return(all.sums)
}

parseYearFolder<-function(root.folder){
  all.folders<-list.dirs(root.folder, recursive=F)
  all.years<-list.dirs(root.folder, recursive=F, full.names = F)
  library(parallel)
  n.cores<-detectCores()-1
  proc.clust<-makeCluster(n.cores, type="FORK")
  all.year.sums<-lapply(all.folders, function(x) countCorpora(x, proc.clust))
  stopCluster(proc.clust)
  names(all.year.sums)<-all.years
  #return(all.year.sums)
  library(plyr)
  final.table<-plyr::ldply(all.year.sums, rbind)
  return(final.table)
}