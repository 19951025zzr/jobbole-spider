# import re
#
# line = '14 评论'
# match_re = re.match(".*?(\d+).*", line)
# print(type(match_re.group(1)))

value = '12 技术'


def convert(value):
    if '评论' in value:
        return ''
    else:
        return value


res = convert(value)

print(res)