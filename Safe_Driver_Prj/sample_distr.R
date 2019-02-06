setwd("C:/Users/lymle/Desktop/Safe_Driver_Prj")
test <- read.csv(file="test.csv", header=TRUE, sep=",")
train <- read.csv(file="train.csv", header=TRUE, sep=",")
output <- read.csv(file="export_pca50.csv", header=TRUE, sep=",")
table(train$target)

output$label <- cut(output$target, c(0, 0.03, 0.04,0.05,0.06, 1))
tbl<-table(output$label)
cbind(tbl,prop.table(tbl))
