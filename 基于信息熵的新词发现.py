import pymysql
import time
import numpy as np
import jieba
class GET_worddics():
    """
    生成格式为‘词：【词频，左边界字分布，右边界字分布】’
    """
    def __init__(self,bookID):
        self.conn = pymysql.connect(host='localhost', user='****', passwd='******', db='novels', charset='utf8')
        self.cur = self.conn.cursor()

        self.bookID = bookID
        self.max_word = 5  # 词语最大长度
        self.min_freq = 5  # 成词的最小频次
        sq = 'select chapID,main_sent,sentID from sentences where chapID in \
                        (select chapID from chapdics where chapdics.bookID="{}") order by chapID ,sentID;'.format(
            self.bookID)
        self.sentences = self.get_sqllists(sq)
    def get_sqllists(self,sq):
        self.cur.execute(sq)
        return list(set(self.cur.fetchall()))
    def get_sideword(self,word,sent, new_dics, i, j):
        if len(word) > 1:
            if sent[i - 1] and i - 1 >= 0:
                new_dics[word][1].append(sent[i - 1])
            elif i == 0:
                new_dics[word][1].append(".")
            if i + j < len(sent):
                if sent[i + j]:
                    new_dics[word][2].append(sent[i + j])
            elif i+j==len(sent):
                new_dics[word][2].append(".")

    def get_s(self):
        sentences=self.sentences
        s=[]
        for chapID, sent, sentID in sentences:
            s.append(sent)
        return s
    def get_rawdic(self):
        """
        生成词及词频字典
        :return:
        """
        max_word = self.max_word
        sentences=self.sentences
        raw_dic = {}
        for chapID, sent, sentID in sentences:
            for i in range(len(sent)):
                for j in range(1, max_word + 1):
                    if i + j < len(sent) + 1:
                        word = sent[i:i + j]
                        if word in raw_dic:
                            raw_dic[word][0] = raw_dic[word][0] + 1
                        else:
                            raw_dic[word] = [1, [], []]
        self.cur.close()
        self.conn.close()
        return raw_dic

    def get_newdic(self):
        new_dics = self.get_rawdic()
        for chapID, sent, sentID in self.sentences:
            for i in range(len(sent)):
                for j in range(1, self.max_word + 1):
                    if i + j < len(sent) + 1:
                        word = sent[i:i + j]
                        if new_dics[word][0] >= self.min_freq:
                            self.get_sideword(word,sent, new_dics, i, j)
        return new_dics


class NEWwords():
    def __init__(self,bookID):
        GET_w=GET_worddics(bookID)
        self.sentences=GET_w.sentences
        self.words_dic =GET_w.get_newdic()  # words_dic[word]=[freq,leftwords,rightwords]
        self.s=GET_w.get_s(self.sentences)  #文本的句子集合
        self.bookID=bookID
        self.conn=pymysql.connect(host='localhost', user='****', passwd='******', db='novels', charset='utf8')
        self.cur=self.conn.cursor()
        self.min_count=5 #最小出现次数
        self.min_support=30 #最低支持度
        self.min_s=3 #最低信息熵，越大越有可能独立成词

        # self.jiebawords=dict.fromkeys(jieba.lcut(' '.join(self.s))) #作用是排除旧词，仅保留新词
    def get_sqllists(self,sq):
        self.cur.execute(sq)
        return list(set(self.cur.fetchall()))
    def get_info(self,words):
        """
        在信息论中，熵是对不确定性的一种度量。信息量越大，不确定性就越小，熵也就越小；信息量越小，不确定性越大，熵也越大。
        :param words:数据库中的格式"'[一,种,度,量]'"，首先需要转换单引号为双引号
        :return:
        """
        # words=json.loads(re.sub('\'','\"',words))
        setwords=set(words)
        info=0
        for word in setwords:
            p=float(words.count(word)/len(words))
            info-=p*np.log2(p)
        return info

    def get_resultdic(self):
        """
        仅保留凝聚度大于30，字数大于2，频率大于5的词
        :return:
        """
        words_dic = self.words_dic
        l = len(''.join(self.s))
        print('文章长度  ',l)
        simpledic = {}
        for w, d in words_dic.items():  # 重置字典为了提升速度
            simpledic[w] = d[0]
        print('凝聚度计算量', len(words_dic.keys()))
        # 凝聚度计算
        result_dic = {}
        # num=0
        for key in list(words_dic.keys()):
            # num+=1
            freq = words_dic[key][0]
            if len(key) > 1 and freq>=self.min_count:

                # 左凝聚程度
                left_nh = (simpledic[key] / (simpledic[key[:1]]* simpledic[key[1:]])) * l
                # 右凝聚程度
                right_nh = (simpledic[key] / (simpledic[key[:-1]] * simpledic[key[-1:]])) * l
                nh = min(left_nh, right_nh)

                if nh >= self.min_support:
                    info = 0
                    leftkeys = words_dic[key][1]
                    rightkeys = words_dic[key][2]
                    result_dic[key] = [freq, info, nh, leftkeys, rightkeys]
                    # if num%10000==0:
                    #     print('已完成  ',(num/len(words_dic.keys())*100),'%')
        return result_dic
    def get_resultdic2(self):
        result_dic=self.get_resultdic()
        print('正在计算信息熵。。。计算量： ',len(result_dic.keys()))
        #信息熵计算
        for word in list(result_dic.keys()):
            try:
                left_info=self.get_info(result_dic[word][3])
                right_info=self.get_info(result_dic[word][4])
                info=min(left_info,right_info)
                # if info>=self.min_s:
                    # print(word)
                result_dic[word][1]=info
            except:
                continue
        return result_dic
    def store(self):
        result_dic=self.get_resultdic2()
        # with open('result_dic.pkl', 'wb') as fw:
        #     fw.write(pickle.dumps(result_dic))
        inserts=[]
        for word,d in result_dic.items():
            if d[1]>=self.min_s:   #仅保留信息熵大于等于3的词
                inserts.append((word,str(d[0]),str(d[1]),str(d[2]),str(self.bookID)))
        print(len(inserts))
        #存储计算结果到mysql
        self.cur.executemany("insert into worddic (word,freq,info,nh,bookID) values(%s,%s,%s,%s,%s)",inserts)
        self.conn.commit()
        self.cur.close()
        self.conn.close()
def get_sqllists(sq,cur):
    cur.execute(sq)
    return list(set(cur.fetchall()))
if __name__=='__main__':
    start=time.clock()
    conn=pymysql.connect(host='localhost', user='****', passwd='******', db='novels', charset='utf8')
    cur=conn.cursor()
    bookIDs=[i[0] for i in get_sqllists('SELECT distinct bookID FROM novels.chapdics;',cur)]
    for bookID in bookIDs:

        NEWwords(bookID).store()
        # jiebawords=[]
        # sentences=GET_worddics(bookID).get_s()
        # for s in sentences:
        #     ws=jieba.lcut(s)
        #     wf=jieba.lcut(s,HMM=False)
        #     if ws!=wf:
        #         print(ws,wf)
        #     jiebawords.append(ws)


    cur.close()
    conn.close()
    print(time.clock()-start)


