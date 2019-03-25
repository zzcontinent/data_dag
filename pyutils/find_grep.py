# -*- coding: utf-8 -*-
import os
import collections


class CFindGrep:
    # None == ''
    def __init__(self, path, file_name=None, line_context=None):
        self.find_list = list()
        self.grep_list = collections.defaultdict(list)

        if file_name is None:
            file_name = ''
        self.__find(file_name, path)

        if line_context is None:
            line_context = ''
        for v in self.find_list:
            self.__grep(line_context, v)

    def get_find_list(self):
        return self.find_list

    def get_grep_list(self):
        return self.grep_list

    def __find(self, name, path):
        if os.path.isfile(path) and name in path:
            self.find_list.append(path)
            return
        for filename in os.listdir(path):
            fp = os.path.join(path, filename)
            if os.path.isfile(fp) and name in filename:
                self.find_list.append(fp)
            elif os.path.isdir(fp):
                self.__find(name, fp)

    def __grep(self, context, path):
        if not os.path.isdir(path):
            self.__grep_file(context, path)
            return
        for filename in os.listdir(path):
            fp = os.path.join(path, filename)
            if os.path.isfile(fp):
                self.__grep_file(context, fp)
            elif os.path.isdir(fp):
                self.__grep(context, fp)

    def __grep_file(self, context, file_full_path):
        with open(file_full_path) as f:
            i = 0
            for line in f:
                i += 1
                if context in line:
                    self.grep_list[file_full_path].append(str(i) + ' : ' + line)

    # 匹配路径+文件内容
    def filter_not(self, context=None):
        if not context:
            return self.grep_list

        outlist = collections.defaultdict(list)
        for v in self.grep_list:
            for line in self.grep_list[v]:
                if context not in line and context not in v:
                    outlist[v].append(line)
        return outlist

    # 匹配路径+文件内容
    def filter_and(self, context=None):
        if not context:
            return self.grep_list

        outlist = collections.defaultdict(list)
        for v in self.grep_list:
            for line in self.grep_list[v]:
                if context in line or context in v:
                    outlist[v].append(line)
        return outlist

    # 匹配路径
    def filter_path_and(self, context=None):
        if context is None:
            return self.grep_list

        outlist = collections.defaultdict(list)
        for v in self.grep_list:
            if context in v:
                outlist[v] = self.grep_list[v]
        return outlist

    # 匹配路径
    def filter_path_not(self, context=None):
        if not context:
            return self.grep_list

        outlist = collections.defaultdict(list)
        for v in self.grep_list:
            if context not in v:
                outlist[v] = self.grep_list[v]
        return outlist

    # 匹配文件内容
    def filter_context_and(self, context=None):
        if not context:
            return self.grep_list

        outlist = collections.defaultdict(list)
        for v in self.grep_list:
            for line in self.grep_list[v]:
                if context in line:
                    outlist[v].append(line)
        return outlist

    # 匹配文件内容
    def filter_context_not(self, context=None):
        if not context:
            return self.grep_list

        outlist = collections.defaultdict(list)
        for v in self.grep_list:
            for line in self.grep_list[v]:
                if context not in line:
                    outlist[v].append(line)
        return outlist


if __name__ == '__main__':
    try:
        x = CFindGrep(r'E:\PyWorkspace', file_name='http', line_context='json')
        x1 = x.filter_and('pyutil')
        x11 = x.filter_path_and('pyutil')
        x111 = x.filter_context_and('pyutil')
        x2 = x.filter_not('pyutil')
        x22 = x.filter_path_not('pyutil')
        x222 = x.filter_context_not('pyutil')
        pass
    except Exception as e:
        print(e)
    finally:
        pass
