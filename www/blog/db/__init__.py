#db.py
import threading;
import mysql.connector;

class _Engine(object):
    def __init__(self, connect):
        self.connect = connect;

engine = None

# 持有数据库连接的上下文对象:
class _DbCtx(threading.local):
    def __init__(self):
        self.connection = None;
        self.transactions = 0;
    
    def is_init(self):
        return not self.connection is None;

    def init(self):
#         self.connection = _LasyConnection();
        self.connection = mysql.connector.\
                connect(user='root',password='password',\
                        database='test', use_unicode=True);
        self.transactions = 0;
    
    def cleanup(self):
        self.connection.cleanup();
        self.connection = None;
    
    def cursor(self):
        return self.connection.cursor();
    
_db_ctx = _DbCtx();

'''
   定义了 __enter__()和__exit__()的对象可以用于 with
   
   with  _ConnectionCtx():
       pass
       
    这是一个连接的嵌套
'''
class _ConnectionCtx(object):
    def __enter__(self):
        global _db_ctx;
        self.should_cleanup = False;
        if not _db_ctx.is_init():
            _db_ctx.init();
            self.should_cleanup = True;
        return self;
    
    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx;
        if self.should_cleanup:
            _db_ctx.cleanup();
    
def connection():
    return _ConnectionCtx();


'''
定义事物的嵌套
'''
class _TransactionCtx(object):
    def __enter__(self):
        global _db_ctx;
        self.shoud_close_conn = False;
        if not _db_ctx.is_init():
            _db_ctx.init();
            self.shoud_close_conn = True;
        _db_ctx.transactions = _db_ctx.transactions + 1;
        return self;
    
    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx;
        _db_ctx.transactions = _db_ctx.transactions - 1;
        try:
            if _db_ctx.transactions == 0:
                if exctype is None:
                    self.commit();
                else:
                    self.rollback();
        finally:
            if self.shoud_close_conn:
                _db_ctx.cleanup();
    
    def commit(self): 
        global _db_ctx;
        try:
            _db_ctx.connection.commit()
        except:
            _db_ctx.connection.rollback();
        
    def rollback(self):
        global _db_ctx;
        _db_ctx.connection.rollback();

'''
    with tranConn() as conn:
        pass
'''
def tranConn():
    return _TransactionCtx();
             
                
        
        