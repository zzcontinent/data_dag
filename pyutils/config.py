# -*- coding: utf-8 -*-
import yaml

class Config:

    def get_yaml(self):
        pass

    def reload(self):
        pass


class ConfigYaml(Config):
    def __init__(self, file_path):
        self.file_path = file_path
        with open(file_path) as in_file:
            self.yaml = yaml.load(in_file)

    def get_yaml(self):
        return self.yaml

    def reload(self):
        with open(self.file_path) as in_file:
            self.yaml = yaml.load(in_file)


class GitInfo:
    def __init__(self, **dict_conf):
        self.repo_url = dict_conf['repo_url']
        self.repo_name = None
        self.user = None
        self.passwd = None
        self.email = None
        if 'repo_name' not in dict_conf:
            i = self.repo_url.rfind('/')
            self.repo_name = self.repo_url[i + 1:]
            ii = self.repo_name.rfind('.git')
            if -1 != ii:
                self.repo_name = self.repo_name[:ii]
        if 'user' in dict_conf:
            self.user = dict_conf['user']
        if 'passwd' in dict_conf:
            self.passwd = dict_conf['passwd']
        if 'email' in dict_conf:
            self.email = dict_conf['email']

        if self.user and self.passwd:
            i = self.repo_url.find('//') + 2
            self.repo_url = self.repo_url[:i] + self.user + ':' + str(self.passwd) + '@' + self.repo_url[i:]


class GitInfoYaml(ConfigYaml):

    def __init__(self, file_path):
        ConfigYaml.__init__(self, file_path)
        self.backup_path = self.get_yaml()['backup_path']
        self.timing_backup = self.get_yaml()['timing_backup']

        self.repos = []
        yaml = self.get_yaml()['repos']
        for v in yaml:
            self.repos.append(GitInfo(**v))


if __name__ == '__main__':
    try:
        # x = ConfigYaml(r'c:\etc\DBConfig_test.yaml')
        x = GitInfoYaml(r'c:\etc\DBConfig_test.yaml')
        pass
    except Exception as e:
        print(e)
    finally:
        pass
