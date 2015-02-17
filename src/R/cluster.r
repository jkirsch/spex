library("blockcluster")
library("Matrix")
# Set working direcotry to current location
#this.dir <- dirname(parent.frame(2)$ofile)
#setwd(this.dir)

setwd("/Work/code/spex/src/R")

# load graph

# Load data
notre = readMM("./../../datasets/webNotreDame.mtx");
notre = readMM("./../../datasets/webBerkStan.mtx");

#image(notre)


#count the number of dangling nodes
rows <- rowSums(notre)

isNull <- function(X)
   { if (X == 0) return (TRUE) else return (FALSE)
   }

isNull = Vectorize(isNull)
filtered <- rows[isNull(rows)]

#show column sum distribution
colsums <- colSums(notre)

notNull <- function(X)
   { if (X >= 0) return (TRUE) else return (FALSE)
   }

notNull = Vectorize(notNull)
filtered <- colsums[notNull(colsums)]


# now try to cluster

B <- as(notre[1:2000,1:2000],"matrix")
out <- cocluster ( B , datatype = "binary" , nbcocluster =c (2 ,3) )
plot(out)


# data ("binarydata")
#> image(binarydata)
#> out <- cocluster ( binarydata , datatype = "binary" , nbcocluster =c (2 ,3) )
#Co-Clustering successfully terminated!
#> plot(out)
#> plot(out)
#> plot(out, type="distribution")
#> plot(out)


# Show the results when reordering
stanford = readMM("./../../datasets/webBerkStan.mtx");

# show without reordering
image(stanford)

image(stanford[, order(-colSums(stanford))])
image(notre[, order(-colSums(notre))])

png("image.png")
# plot
image(stanford)
# save
dev.off()


