def literal(x):
    if type(x) == str:
        return 'Literal_'
    else:
        return x.is_a


def change_prefix(data_path, s):
    s = str(s)
    s_list = s.rsplit('.',1) 

    if data_path[:-3] in s:
        s = s.replace(data_path[:-3],'skmo:')
    else:
        s = s_list[0] + ':' + s_list[-1]
    return s