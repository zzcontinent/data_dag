# -*- coding: utf-8 -*-
import xml.etree.ElementTree as ET


class CXML:
    def __init__(self, infilePath=None):
        if infilePath != None:
            self.m_tree = ET.parse(infilePath)

    def getTree(self):
        return self.m_tree

    def getRoot(self):
        return self.getTree().getroot()

    def getNode(self, inTree, inNodeName, inDict):
        list = inTree.getiterator(inNodeName)
        for node in list:
            iFoundCnt = 0
            for label in node:
                for k, v in inDict.iteritems():
                    if label.tag == k and label.text == v:
                        iFoundCnt += 1
            if iFoundCnt == len(inDict):
                return node
        return None

    def getNodeList(self, inTree, inNodeName):
        return inTree.getiterator(inNodeName)

    def getNodeKV(self, inNode):
        outDict = {}
        for label in inNode:
            outDict[label.tag] = label.text
        return outDict

    def modNode(self, inNode, inModDict):
        if inNode == None:
            return
        for label in inNode:
            for k, v in inModDict.items():
                if label.tag == k:
                    label.text = v

    def appendNode(self, inTree, inFatherName, inAddNode):
        if inTree == None or inAddNode == None:
            return
        listNode = self.getNodeList(inTree, inFatherName)
        for node in listNode:
            node.append(inAddNode)

    def removeNode(self, inTree, inFatherName, inRmNode):
        fatherList = self.getNodeList(inTree, inFatherName)
        for fatherNode in fatherList:
            if inRmNode in fatherNode:
                fatherNode.remove(inRmNode)

    def makeTree(self, inFatherTag, **inChildKV):
        father = ET.Element(inFatherTag)
        for k, v in inChildKV.iteritems():
            child = ET.SubElement(father, k)
            child.text = v
        return father

    def writeXml(self, inPath):
        """格式化root转换为xml文件"""
        self.m_tree.write(inPath, encoding="utf-8", xml_declaration=True)
