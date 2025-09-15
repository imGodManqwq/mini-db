# Mini-DB �洢ϵͳ

## ����

����һ���򻯰�����ݿ�洢ϵͳ��ʵ����ҳʽ�洢��������ƺͻ����ı��������ܡ�

## ���Ĺ���

### 1. ҳʽ�洢 (Page Storage)
- **Page��**: ʵ��������ҳʽ�洢����
  - ֧��ҳ���䡢�ͷš���д����
  - ҳ���С�����ã�Ĭ��4096�ֽڣ�
  - ֧����ҳ��Ǻ����л�/�����л�
  - �ṩҳ��ռ����

### 2. ������� (Buffer Pool)
- **LRU/FIFO����**: ֧�����ֻ����滻����
  - LRU (Least Recently Used): �������ʹ��
  - FIFO (First In First Out): �Ƚ��ȳ�
- **����ͳ��**: ʵʱͳ�ƻ��������ʡ����������
- **��־����**: ��ϸ�Ĳ�����־��¼
- **����ģ��**: ҳ��־û��������ļ�

### 3. �������ӿ� (Table Operations)
- **��¼����**: ���롢��ѯ�����¡�ɾ����¼
- **ҳ����**: �Զ�ҳ����Ϳռ����
- **���л�**: ��¼���ݵ����л��ͷ����л�
- **ͳ����Ϣ**: �������ͳ�ƺͼ��

## ϵͳ�ܹ�

```
��������������������������������������
��   Table Layer   ��  �� �߼��������ӿ�
��������������������������������������
��  Buffer Pool    ��  �� ���������ҳ����
��������������������������������������
��   Page Layer    ��  �� ҳʽ�洢����
��������������������������������������
��   Disk Layer    ��  �� �־û��洢
��������������������������������������
```

## ��Ҫ��˵��

### Page��
```cpp
class Page {
    // ���Ĺ���
    bool writeData(int offset, const void* data, int size);
    bool readData(int offset, void* data, int size) const;
    
    // ��������
    int getPageID() const;
    int getFreeSpace() const;
    bool isDirty() const;
    void setDirty(bool dirty);
};
```

### BufferPool��
```cpp
class BufferPool {
    // �������
    Page* getPage(int pageID);
    void flushPage(int pageID);
    void flushAll();
    
    // ҳ����
    int allocatePage();
    void deallocatePage(int pageID);
    
    // ͳ�ƺ���־
    CacheStats getStats() const;
    void enableLogging(const std::string& logFile);
};
```

### Table��
```cpp
class Table {
    // ��¼����
    int insertRecord(const std::vector<std::string>& record);
    Record* queryRecord(int recordID);
    bool updateRecord(int recordID, const std::vector<std::string>& newRecord);
    bool deleteRecord(int recordID);
    
    // �洢�ӿ�
    void flushAll();
    void printTableInfo();
};
```

## ʹ��ʾ��

```cpp
// ������
Table table("StudentRecords", 4096, 3);

// �����¼
std::vector<std::string> record = {"Alice", "Johnson", "Computer Science", "A"};
int recordID = table.insertRecord(record);

// ��ѯ��¼
Record* found = table.queryRecord(recordID);

// ���¼�¼
std::vector<std::string> updated = {"Alice", "Johnson", "Computer Science", "A+"};
table.updateRecord(recordID, updated);

// ɾ����¼
table.deleteRecord(recordID);

// �鿴ͳ����Ϣ
table.printTableInfo();
```

## ��������

### ��������
- **������ͳ��**: ʵʱ��ػ���������
- **�������**: LRU/FIFO����ҳ���滻
- **Ԥ������**: ֧��ҳ��Ԥ����

### �洢Ч��
- **ҳʽ����**: �̶���Сҳ�棬���ڹ���
- **�ռ临��**: �Զ����պ�����ҳ��ռ�
- **��������**: ֧������ˢ�º�ͬ��

## ���Խ��

���в��Գ�����Կ�����
- ����������ͳ��
- ҳ�������������
- ����I/O������¼
- ����������ָ��

## �ļ��ṹ

```
mini-db/
������ include/storage/
��   ������ Page.h          # ҳʽ�洢ͷ�ļ�
��   ������ BufferPool.h    # �����ͷ�ļ�
��   ������ Table.h         # ������ͷ�ļ�
������ src/storage/
��   ������ Page.cpp        # ҳʽ�洢ʵ��
��   ������ BufferPool.cpp  # �����ʵ��
��   ������ Table.cpp       # ������ʵ��
������ data/               # �����ļ�Ŀ¼
��   ������ page_*.dat      # ҳ�������ļ�
������ buffer_pool.log     # ������־�ļ�
```

## ��չ��

��ϵͳ��ƾ������õ���չ�ԣ�
- �����������µĻ������
- ֧�ֲ�ͬ��ҳ���С����
- ����չΪ�����Ĵ��̴洢
- ֧�ֲ������ʿ���
- �ɼ��������Ͳ�ѯ�Ż�

## ���������

```bash
# ����
cd build
cmake ..
cmake --build . --config Debug

# ���в���
./Debug/mini_db.exe
```

## ע������

1. ��ǰ�汾ʹ���ļ�ϵͳģ����̴洢
2. ��¼���л�ʹ�ü򵥵Ķ����Ƹ�ʽ
3. ҳ���С�̶�Ϊ4096�ֽ�
4. ��������ڴ���ʱȷ��������ʱ���ɸ���
5. ��־�ļ��������������Ҫ��������



