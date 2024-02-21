import re
from ckip_transformers.nlp import CkipWordSegmenter, CkipPosTagger



def check_english(x):
    my_re = re.compile(r'[A-Za-z]',re.S) #使 . 匹配包括换行在内的所有字符
    res = re.findall(my_re,x)
    if len(res):
        return True
    else:
        return False
    
def get_maxlen_english(x):#最長純英文
    s_en=re.sub(r"[^a-zA-Z ]+",' ',x)
    res=max(s_en.split(' '),key=len,default='')
    return res

def get_maxlen_ennum(x):#最長英文 數字
    s_en=re.sub(r"[^a-zA-Z0-9 ]+",' ',x)
    res=max(s_en.split(' '),key=len,default='')
    return res

def remove_punctuations(s):  #去除標點符號和數字
        rule = "[^\u4e00-\u9fa5']"
        rule = re.compile(rule)
        s = s.replace("’","'")
        s = rule.sub(' ',s)
        s = ' '.join(s.split())
        return s


def strQ2B(s):
    """
    turn all the fullwidth into halfwidth
    """
    rstring = ""
    for uchar in s:
        u_code = ord(uchar)
        if u_code == 12288:  # 全形空格直接轉換
            u_code = 32
        elif 65281 <= u_code <= 65374:  # 全形字元（除空格）根據關係轉化
            u_code -= 65248
        rstring += chr(u_code)
    return rstring


def remove_words(s):
    # remove weight
    weight = re.compile("[0-9]+[Gg]+")
    # remove price
    price = re.compile("[0-9]+元")

    package = re.compile("[0-9]+入")

    # remove ml
    capacity = re.compile("[0-9]+[ml]", re.IGNORECASE)

    # remove special tags
    sTags = re.compile("《.*》")

    # 去除西元年
    WesternYear1 = re.compile("[0-9]{4}年")
    WesternYear2 = re.compile("20[0-9]{2}")
    # 去除民國年
    ROCYear = re.compile("1[0-9]{2}年")
    # 去除%數
    percent = re.compile("[0-9]{1,3}%")

    # remove others
    re1 = re.compile("\(O\)")
    re2 = re.compile("\(高\)")
    re3 = re.compile("\(保\)")
    re4 = re.compile("\(區\)")
    re5 = re.compile("\(代銷\)")
    re6 = re.compile("\(特殊銷\)")
    re7 = re.compile("\(隨\)")
    re8 = re.compile("\(贈\)")
    re9 = re.compile("\(預\)")
    re10 = re.compile("宅配")
    re11 = re.compile("代售商品[一二三]")
    re12 = re.compile("[1-9][包盒組片]")
    re13 = re.compile("他店取貨")
    re14 = re.compile("條碼")
    re15 = re.compile("任選")
    re16 = re.compile("他店取貨")
    re17 = re.compile("隨買跨店取應稅[1-3]{1}")
    re18 = re.compile("NF測試商品")
    re19 = re.compile("[一二]配")
    re20 = re.compile("獨家")
    re21 = re.compile("免稅")
    re22 = re.compile("隨買預約取應稅[1-3]{1}")
    re23 = re.compile("預購訂購到店應稅")
    re24 = re.compile("日翊常溫[1-9]{1}")
    re25 = re.compile("應稅")
    re26 = re.compile("團購加工[1-9]{1}")
    re27 = re.compile("日翊配送加工[1-9]{1}")
    re28 = re.compile("\(宅\)")
    re29 = re.compile("它 ")
    re30 = re.compile("他 ")
    re31 = re.compile("團 ")
    re32 = re.compile("x([0-9a-fA-F]{4})")
    re33 = re.compile("\n")
    re34 = re.compile("代銷")
    re35 = re.compile("代售商品[一二三]")
    re36 = re.compile("[團預]購")
    re37 = re.compile("[係系]列")
    re38 = re.compile("草莓季")
    re39 = re.compile("[隨髓]買跨店取[123]")
    re40 = re.compile("[1-9][杯顆入]")
    re41 = re.compile("\(跨\)")
    re42 = re.compile("其[他它]")
    re43 = re.compile("試吃品")
    re44 = re.compile("\(包\)")
    # remove special symbols
    sym = re.compile(r"[【.*】\-\–\—\?\\\《\》\&\(\)\_\^★、!±『』]")

    re_list = [weight, package, price, capacity, sTags, re1,
               re2, re3, re4, re5, re6, re7, re8, re9, re10, re11,
               re12, re13, re14, re15, re16, re17, re18, re19,
               re20, re21, re22, re23, re24, re25, re26, re27,
               re28, re29, re30, re31, re32, re33, re34, re35,
               re36, re37, re38, re39, re40, re41, re42, re43,re44,
               sym, WesternYear1, WesternYear2, ROCYear, percent]

    for r in re_list:
        s = re.sub(r, "", s)
    return s


def proccessing(s):
    if s != "" and type(s) == str:
        s = strQ2B(s).lower()
        s = remove_words(s)
        s = s.strip()
    return s


def preserve_nounce(ws, pos):

  from itertools import compress
  products = []

  for sentence_ws, sentence_pos in zip(ws, pos):
    x = compress(sentence_ws, [s.startswith("N") and s != 'Neu' and s != 'Nf' for s in sentence_pos])
    products.append("".join(list(x)))
  products = "".join(products)

  return products


class CKIP_clean():

    def __init__(self, df, col: str):
        self.df = df
        self.col = col

    def _load_models(self):
        ws_driver = CkipWordSegmenter(model_name=r"CKIP_models/ws_model", device=-1)
        pos_driver = CkipPosTagger(model_name=r"CKIP_models/bert-base-chinese-pos", device=-1)
        return ws_driver, pos_driver

    def fea_process(self):
        fea = self.df[self.col].astype(str)
        fea = fea.str.replace("nan", "")
        return fea

    def run(self):
        ws_driver, pos_driver = self._load_models()
        ws = ws_driver(self.fea_process(), use_delim=True)
        pos = pos_driver(ws, use_delim=True)

        noun_only = preserve_nounce(ws, pos)

        return noun_only
