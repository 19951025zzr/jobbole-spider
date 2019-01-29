import hashlib


def get_md5(url):
    # 判断传入的URL是否为unicode
    if isinstance(url, str):
        url = url.encode('utf-8')
    m = hashlib.md5()
    m.update(url)
    return m.hexdigest()


if __name__ == '__main__':
    print(get_md5('http://jobbole.com'.encode('utf-8')))