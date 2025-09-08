a mini database
参考架构
mini_db/ 
│ 
├─ src/
│ ├─ parser/ # SQL 编译器 
│ │ ├─ Lexer.h # 词法分析
│ │ ├─ Parser.h # 语法分析 
│ │ └─ AST.h # 抽象语法树 / 逻辑计划 
│ │ 
│ ├─ storage/ # 存储系统 
│ │ ├─ Table.h # 表定义 
│ │ ├─ Page.h # 页式存储（可扩展成磁盘页） 
│ │ ├─ BufferPool.h # 缓冲池管理 
│ │ └─ Index.h # B+树索引（进阶） 
│ │ 
│ ├─ executor/ # 执行引擎 
│ │ ├─ Executor.h # 执行器调度 
│ │ ├─ Operators.h # 扫描 / 过滤 / 投影 / 连接等算子 
│ │ └─ Optimizer.h # 查询优化（谓词下推、索引选择） 
│ │ 
│  db/ 
│ │ └─ MiniDB.h # 封装整个数据库接口 
│ │ 
│ ├─ cli/ 
│ │ └─ REPL.h # 命令行交互接口 
│ │
│ └─ tests/ # 单元测试 
│   ├─ ParserTest.cpp 
│   ├─ StorageTest.cpp 
│   ├─ ExecutorTest.cpp 
│   └─ IntegrationTest.cpp 
│ 
└─ main.cpp # 入口（启动 MiniDB 或 REPL）