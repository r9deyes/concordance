﻿# -*- coding: utf-8 -*-
import time
def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        print("%r %2.2f sec"%(method.__name__, te-ts))
        return result
    return timed

class concordance:
    import re
    endFlag=False
    buffer=''
    listBuffer=['']
    sepByWords=False    #отделять слова по разделителям(F) или по шаблону слов(T)
    lenByLetters=False  #считать длину посимвольно(T) или по словам(F)
    sourceName=None
    storage='./concordance.csv'
    def __init__(s,key,sourceName,lenL=17,lenR=17,saveTo='list',separate=' ',sepByWords=False, lenByLetters=False):
        try:
            if not(isinstance(key,str)):
                key.match('')
        except AttributeError:
            print('Need str or regex key!')
            raise(AttributeError)# rex is not an re
        else:
            s.key=key
        s.lenR=lenR
        s.lenL=lenL
        s.sourceName=sourceName
        s.saveTo=saveTo
        try:
            if not(isinstance(separate,str)):
                separate.split('')
        except AttributeError:
            print('Need str or regex key!')
            raise(AttributeError)# rex is not an re
        else:
            s.separate=separate
        if(saveTo == 'list'):
            s.storage=[]
        if(saveTo == 'file'):
            s.storage='./concordance.csv'
        s.startFlag = True
        s.sepByWords=sepByWords
        s.lenByLetters=lenByLetters

    def add_record(s,record):
        if(s.saveTo=='list'):
            s.storage.append(record)
        if(s.saveTo=='file'):
            with open(s.storage,mode='ab') as fstor:
                fstor.write(record)

    def check_word(s,word):
        if(isinstance(s.key,str)):
            return s.key==word
        else:
            return bool(s.key.match(word))
        
    def split(s,string):
        if(isinstance(s.separate,str)):
            return string.split(s.separate)
        else:
            res=s.separate.split(string)
            return [res[i] for i in range(len(res)) if res[i]!='' or i==(len(res)-1) or i==0]

    def _len(s,string):
        if(s.lenByLetters):
            return len(string)
        else:
            return len(s.split(string))
    @timeit
    def full_parsing(s,string):
        words = s.split(string)
        for wi in range(len(words)):
            if(s.check_word(words[wi])):
                k=wi-s.lenL#int(((wi-s.length)+abs(wi-s.length))/2)
                if(k<=0):k=0
                record = [words[k:wi], words[wi],words[wi+1:s.lenR+wi+1]]
                s.add_record(record)
        return s.storage

    def listStream_parsing(s,stream_string):
        list_stream_string=s.split(stream_string)
        listString = s.listBuffer[:-1]+[s.listBuffer[-1]+list_stream_string[0]]+list_stream_string[1:]
        if(len(listString)<=s.lenR+s.lenL and not s.lenByLetters):
            s.listBuffer=listString
        else:
            lastCheckedKeyIndex=None
            for wi in range(len(listString)-1):
                if(s.check_word(listString[wi]) and (s.lenR+wi+1)<len(listString)):
                    k=wi-s.lenL
                    if(k<=0):k=0
                    record = [listString[k:wi], listString[wi],listString[wi+1:s.lenR+wi+1]]
                    lastCheckedKeyIndex=wi
                    s.add_record(record)
            if(lastCheckedKeyIndex is not None):
                wi=lastCheckedKeyIndex
                if((s.lenR+wi+1)>len(listString)):
                    s.listBuffer=listString[k:wi]+ [listString[wi]]+listString[wi+1:]
                else:
                    s.listBuffer=listString[s.lenR+wi+1:]
            else:
                s.listBuffer=listString[-(s.lenL+1+s.lenR):]
        return s.storage

    def closeConc(s):
        s.listBuffer+=['']*s.lenR;
        s.listStream_parsing('')
        s.storage[-1][2]=[i for i in s.storage[-1][2] if i!='']
        return s.storage

    
    def stream_parsing(s,stream_string):
        string=s.buffer+stream_string
        string_len=s._len(string)
        buf_len=s._len(s.buffer)
        if(string_len<=s.lenR+s.lenL):
            s.buffer=string
        else:
            words = s.split(string)
            lastCheckedKeyIndex=None
            for wi in range(len(words)-1):
                if(s.check_word(words[wi]) and 1):
                    k=wi-s.lenL
                    if(k<=0):k=0
                    record = [words[k:wi], words[wi],words[wi+1:s.lenR+wi+1]]
                    lastCheckedKeyIndex=wi
                    s.add_record(record)
            if(lastCheckedKeyIndex>=0):
                s.buffer=' '.join(words[lastCheckedKeyIndex:]) #WRONG!!! 
        return s.storage
    

if __name__ == '__main__':
#if 0:
    import re
    
    test1 = concordance(key=re.compile(u'выбор'),sourceName='test_menejment_bystream',lenL=3,lenR=3,separate=re.compile('[\s\:\.\,]'))
    test2 = concordance(key=re.compile(u'выбор'),sourceName='test_menejment_byFull',lenL=3,lenR=3,separate=re.compile('[\s\:\.\,]'))
    txt=''
    
    @timeit
    def f1(txt,test1):
        j,i,k=-1,0,0
        for t,i in txt,range(len(txt)):
            #if(test1.separate.match(t[-1])):
            #    k+=1
            i=len(test1.listStream_parsing(t))
            if i>j:
                j=i
                if(i==13):
                    print(ti)
                    print("\n"+str(i)+"__"+t+"\n")
                else:
                    print(t)
    
    
    #with open('C:/users/DAN85_000/Downloads/do.vgkuint.ru/moodledata/15/backupdata/quiz/quiz-menedzhment-default_for_menedzhment-20151118-1229.txt','rb') as f:
    with open('C:/users/DAN85_000/Downloads/db_2016-06-16_01-08/a0079656_efr.sql','rb') as f:
        text=f.read().decode('utf-8')
    c2=test2.full_parsing(text)
    #for testSize in range(255,15000,255):
    testSize=255
    if(1):
        test1 = concordance(key=re.compile(u'выбор'),sourceName='test_menejment_bystream',lenL=3,lenR=3,separate=re.compile('[\s\:\.\,]'))
        strSize=testSize
        txt=[text[i:i+strSize] for i in range(0,len(text),strSize)]
    #text='test word rr part and something new qq ww ee rr ss'
   # txt=re.compile('.{25}').findall(text)
    #print(txt)
        k=0
        f1(txt,test1)
        c1=test1.closeConc()
        print("parts: "+str(len(txt))+" size: "+str(testSize)+" right?: "+str(c1==c2))
    if(c1!=c2):
        print("len_c1: "+str(len(c1))+" len_c2: "+str(len(c2)))
        for i in range(max((len(c1),len(c2)))):
            if(c1[i]!=c2[i]):
                print(i)
                if(c1[i]==c2[i+1]):
                    print('+'+str(i))
                if(c1[i]==c2[i-1]):
                    print('-'+str(i))

    #c=test.full_parsing('test word rr part and something new qq ww ee rr ss')
    #print(str(len(txt))+' '+str(len(text))+' '+str(k))
    #print(c1==c2)
    #print('===========================')
    #print(c2)
#    print(txt[:17])
