library("blockcluster")
library("Matrix")
# Set working direcotry to current location
#this.dir <- dirname(parent.frame(2)$ofile)
#setwd(this.dir)

setwd("/Work/code/spex/src/R")

# load graph

# Load data
notre = readMM("./../../datasets/webNotreDame.mtx");
#image(notre)

# now try to cluster

B <- as(notre[1:2000,1:2000],"matrix")
out <- cocluster ( B , datatype = "binary" , nbcocluster =c (2 ,3) )
plot(out)

