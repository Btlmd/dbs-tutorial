import random
import os
from tqdm import tqdm

with open(os.path.join(os.path.dirname(__file__), "wordlist.txt"), "r") as f:
    word_list = f.read().split()

random.seed(4022)

def i(*rg):
    if len(rg) == 2:
        return random.randint(*rg)
    return random.randint(0, 40)

def f():
    return round(random.random() * 100, 3)

def s(max_len):
    def _s():
        ret = []
        r_len = random.random() * max_len
        while sum(map(len, ret)) + len(ret) - 1 < r_len:
            ret.append(random.choice(word_list))
        while sum(map(len, ret)) + len(ret) - 1 > max_len:
            ret.pop()
        return " ".join(ret)
    return _s

def to_record(fields):
    out = []
    for f in fields:
        if isinstance(f, str):
            out += ["'" + f + "'"]
        else:
            out += [str(f)]
    # print(out)
    return "(" + ", ".join(out) + ")"

insert = "INSERT INTO %s VALUES %s;\n"

def gen_insertions(template_func, name, count, show_tqdm=False):
    _sql = []
    it = range(count)
    if show_tqdm:
        it = tqdm(it)
    for j in it:
        _sql += [to_record([x() for x in template_func])]
    _sql = ", ".join(_sql)
    _sql = insert % (name, _sql)
    return _sql