# -*- coding: utf8 -*-
import operator
from math import log

__author__ = 'linzhou208438'
__update__ = '2015/7/14'


class DecisionTree(object):
    '''
    信息增益=香农熵-条件熵
    条件熵 = X集合在yj条件下的条件熵为: H(X|yj)=∑p(xi|yj)I(xi|yj)=-∑p(xi|yj)logp(xi|yj)
    香农熵 = 公式表达为: H(x)=∑p(xi)I(xi)=-∑p(xi)logp(xi)
    '''

    def createDataSet(self):
        '''
        创建测试数据
        创建矩阵,矩阵行向量前两列是特征值,最后一列是最终结果
        '''
        dataSet = [[1, 1, 'yes'],
                   [1, 1, 'yes'],
                   [1, 0, 'no'],
                   [0, 1, 'no'],
                   [0, 1, 'no']]
        return dataSet


    def calcShannonEnt(self,dataSet):
        '''
        计算香农熵
        此过程按照我们上面描述的公式进行计算
        1.获取矩阵行向量数量(此处是通过二维数组模拟矩阵,所以不可以通过shape[0]获取行向量数量)
        2.遍历所有行向量,统计每一个向量"结果"出现的次数
        3.遍历所有结果,计算每个结果比例以及对应的香农熵(公式表达为: H(x)=∑p(xi)I(xi)=-∑p(xi)logp(xi))
        4.输入结果
        '''
        numEntries = len(dataSet)
        labelCounts = {}
        for featVec in dataSet:
            currentLabel = featVec[-1]
            if currentLabel not in labelCounts.keys():
                labelCounts[currentLabel] = 0
            labelCounts[currentLabel] += 1
        shannonEnt = 0.0
        for key in labelCounts:
            prob = float(labelCounts[key])/numEntries
            shannonEnt -= prob * log(prob, 2) #log base 2
        return shannonEnt

    def splitDataSet(self,dataSet, axis, value):
        '''
        划分数据集
        删除某个行向量中列索引为axis,值为value的列,最终返回所有行向量集合
        '''
        retDataSet = []
        for featVec in dataSet:
            if featVec[axis] == value:
                reducedFeatVec = featVec[:axis]
                reducedFeatVec.extend(featVec[axis+1:])
                retDataSet.append(reducedFeatVec)
        return retDataSet


    def chooseBestFeatureToSplit(self,dataSet):
        '''
        选择最优特征
        最优特征本质上是计算信息增益
        1.获取特征数量
        2.计算香农熵
        3.遍历特征:
            3.1 获取所有样本特征值集合
            3.2 获取特征值种类
            3.3 遍历每一个特征值
                3.3.1 根据当前特征值划分数据集
                3.3.2 计算当前数据集数量/总的样本数量,目的是得到当前特征值占所有值的比例
                3.3.3 计算该特征值的香农熵
                3.3.4 计算该特征值的条件熵(将3.3.2和3.3.3的结果相乘)
                3.3.5 累加该特征值的条件熵到特征熵结果中
            3.4 计算信息增益,用基本熵减去特征熵
            3.5 如果当前信息增益最大则保留同时记录特征索引号
        4.返回特征索引号
        '''
        numFeatures = len(dataSet[0]) - 1      #the last column is used for the labels
        baseEntropy = self.calcShannonEnt(dataSet)
        bestInfoGain = 0.0
        bestFeature = -1
        for i in range(numFeatures):
            featList = [example[i] for example in dataSet]
            uniqueVals = set(featList)       #get a set of unique values
            newEntropy = 0.0
            for value in uniqueVals:
                subDataSet = self.splitDataSet(dataSet, i, value)
                prob = len(subDataSet)/float(len(dataSet))
                newEntropy += prob * self.calcShannonEnt(subDataSet)
            infoGain = baseEntropy - newEntropy     #calculate the info gain; ie reduction in entropy
            if (infoGain > bestInfoGain):       #compare this to the best gain so far
                bestInfoGain = infoGain         #if better than current best, set to best
                bestFeature = i
        return bestFeature

    def majorityCnt(self,classList):
        '''
        获取最优结果
        给定一个包含多个值的结果集,选择值最多的结果并返回
        '''
        classCount={}
        for vote in classList:
            if vote not in classCount.keys(): classCount[vote] = 0
            classCount[vote] += 1
        sortedClassCount = sorted(classCount.iteritems(), key=operator.itemgetter(1), reverse=True)
        return sortedClassCount[0][0]

    def createTree(self,dataSet,labels):
        '''
        构建决策树
        1.创建结果列表
        2.如果所有结果都相同则直接返回第一个值
        3.如果没有特征值只剩下结果一列则选择结果最多的名称
        4.选择最优特征
        5.选择最优秀特征值标签
        6.初始化决策树,里面只有一个属性既特征值标签,且属性值为空字典
        7.在标签列表中删除当前最优特征值标签
        8.取当前样本集的最优特征的所有值列表
        9.将上述列表转换为集合,得到唯一值
        10.遍历集合
            10.1 拷贝所有标签
            10.2 以特征名称和当前值作为key构造树结果
        '''
        classList = [example[-1] for example in dataSet]
        if classList.count(classList[0]) == len(classList):
            return classList[0]#stop splitting when all of the classes are equal
        if len(dataSet[0]) == 1: #stop splitting when there are no more features in dataSet
            return self.majorityCnt(classList)
        bestFeat = self.chooseBestFeatureToSplit(dataSet)
        bestFeatLabel = labels[bestFeat]
        myTree = {bestFeatLabel:{}}
        del(labels[bestFeat])
        featValues = [example[bestFeat] for example in dataSet]
        uniqueVals = set(featValues)
        for value in uniqueVals:
            subLabels = labels[:]       #copy all of labels, so trees don't mess up existing labels
            myTree[bestFeatLabel][value] = self.createTree(self.splitDataSet(dataSet, bestFeat, value),subLabels)
        return myTree

    def classify(self,inputTree,featLabels,testVec):
        '''
        测试决策树
        1.获取决策树第一个属性key
        2.获取决策树第一个属性对应的值value
        3.获取特征标签的索引号
        4.根据第三步的索引号获取待测试向量的对应特征的值
        5.获取该特征值对应的value
        6.如果该结果是dict类型则继续递归调用classify,否则结束执行
        '''
        firstStr = inputTree.keys()[0]
        secondDict = inputTree[firstStr]
        featIndex = featLabels.index(firstStr)
        key = testVec[featIndex]
        valueOfFeat = secondDict.get(key)
        if isinstance(valueOfFeat, dict):
            classLabel = self.classify(valueOfFeat, featLabels, testVec)
        else: classLabel = valueOfFeat
        return classLabel

if __name__ == '__main__':
    decisionTree=DecisionTree()
    labels = [0,1]
    dataSet = decisionTree.createDataSet()
    inputTree = decisionTree.createTree(dataSet,labels)
    print inputTree,labels
    testVec = [0, 1, 'no']

    classLabel= decisionTree.classify(inputTree,[0,1],testVec)
    print classLabel





