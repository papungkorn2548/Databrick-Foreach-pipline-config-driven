# Databrick-Foreach-pipline-config-driven

โปรเจกต์นี้เป็น **Data Pipeline แบบ Config-Driven** บน Databricks ที่ใช้สถาปัตยกรรม Medallion (Bronze → Silver → Gold) ร่วมกับความสามารถ `for_each_task` ของ Lakeflow Jobs เพื่อรันหลายไปป์ไลน์พร้อมกันแบบขนาน โดยอ้างอิงการตั้งค่าจาก **Config Table** ใน Unity Catalog ไม่ต้องแก้โค้ดเมื่อเพิ่มแหล่งข้อมูลใหม่ เพียงแค่เพิ่มแถวในตาราง config

## จุดเด่น

* **Config-Driven** — เพิ่ม/แก้ไขไปป์ไลน์ได้โดยไม่ต้องแก้โค้ด เพียงแก้ไขข้อมูลใน Config Table
* **For-Each Parallelism** — ใช้ `for_each_task` รัน Bronze และ Silver ของทุกไปป์ไลน์พร้อมกัน
* **Medallion Architecture** — แยกชั้นข้อมูล Bronze (Raw) → Silver (Cleansed) → Gold (Aggregated)
* **Data Quality Framework** — ตรวจสอบ schema, null key, duplicate และแยก bad records อัตโนมัติ
* **SCD Type 2** — รองรับ Slowly Changing Dimension Type 2 ด้วย hash-based change detection
* **Reusable Python Package** — รวม logic ไว้ใน `logic_packages` ที่ build เป็น `.whl` ได้
* **Declarative Automation Bundles (DABs)** — deploy ได้ทั้ง target `dev` และ `prod`

## โครงสร้างโปรเจกต์

```
.
├── databricks.yml              # DABs bundle config (dev / prod targets)
├── requirements.txt            # Python dependencies
├── resources/
│   └── workflow.yml            # Job definition (for_each_task, task dependencies)
├── src/
│   ├── framework               # Notebook รวม class BronzeLayer & SilverLayer
│   ├── Bronze/                 # Bronze layer notebooks
│   │   ├── Task_foreach_bronze_data   # อ่าน config table → ส่งค่าไปยัง for_each
│   │   └── Bronze_fw                 # รัน BronzeLayer ตาม config ที่รับมา
│   ├── Silver/                 # Silver layer notebooks
│   │   ├── Silver_fw                 # รัน SilverLayer (DQ + SCD2)
│   │   ├── SCD2_fw                   # ประมวลผล SCD Type 2
│   │   └── silver_cross_check        # ตรวจสอบข้อมูล Silver
│   └── Gold/
│       └── GOLD                     # Gold layer notebook
├── config_table/
│   └── ddl                     # สร้าง catalog, schema, volume และ config_table
├── data_set/                   # ไฟล์ CSV ตัวอย่าง (orders, customers, products, ...)
├── logic_packages/             # Python package สำหรับ reuse logic
│   ├── pyproject.toml
│   └── src/unified_transform_logic/
│       ├── __init__.py
│       └── framework_test.py    # BronzeLayer & SilverLayer (local test ได้)
└── tests/
    └── test_transform.py       # Unit tests
```

## ข้อกำหนดเบื้องต้น (Prerequisites)

* **Databricks Workspace** พร้อม Unity Catalog เปิดใช้งาน
* **Databricks CLI** (สำหรับ deploy bundle)
* **Python 3.10+** (สำหรับ build `.whl` ใน target prod)
* แพ็กเกจ Python ตาม `requirements.txt`:
  * `pyspark==3.5`
  * `delta-spark==3.2.0`
  * `build==1.5.0`

## การติดตั้ง (Installation)

### 1. Clone โปรเจกต์

```bash
git clone <repository-url>
cd Databrick-Foreach-pipline-config-driven
```

### 2. ติดตั้ง Databricks CLI (ถ้ายังไม่มี)

```bash
pip install databricks-cli
databricks configure
```

### 3. สร้าง Config Table และโครงสร้าง Unity Catalog

รัน notebook `config_table/ddl` บน Databricks เพื่อสร้าง:

* Catalog: `session_life_hamham`
* Schema: `session_life`
* Volume: `manual_file_folder` (สำหรับเก็บไฟล์ CSV)
* Table: `config_table` (ตาราง config หลัก)

จากนั้น insert ข้อมูล config ของแต่ละไปป์ไลน์ลงใน `config_table` โดยแต่ละแถวประกอบด้วย:

| คอลัมน์ | คำอธิบาย | ตัวอย่าง |
| --- | --- | --- |
| `pipeline_name` | ชื่อไปป์ไลน์ | `orders` |
| `file_path` | path ของไฟล์ต้นทาง | `/Volumes/.../orders.csv` |
| `header` | มี header หรือไม่ | `true` |
| `delimiter` | ตัวคั่น | `,` |
| `table_name` | ชื่อ Delta table เป้าหมาย | `session_life_hamham.session_life.orders` |
| `schema_detail` | map ของคอลัมน์และประเภท | `{"order_id": "int", ...}` |
| `keys` | array ของ primary key | `["order_id"]` |
| `write_mode` | โหมดการเขียน | `overwrite` |
| `source_name` | ตาราง Silver ต้นทาง (สำหรับ SCD2) | `session_life_hamham.session_life.employee` |
| `scd2_enabled` | เปิด SCD Type 2 หรือไม่ | `true` |

### 4. อัปโหลดไฟล์ CSV ตัวอย่าง

อัปโหลดไฟล์ในโฟลเดอร์ `data_set/` ไปยัง Volume `session_life_hamham.session_life.manual_file_folder`

## วิธีการใช้งาน (Usage)

### รัน Setup Notebook (DDL)

ก่อนรัน pipeline ครั้งแรก ต้องรัน notebook `config_table/ddl` เพื่อสร้าง catalog, schema, volume, config table และ copy ไฟล์ CSV ไปยัง Volume:

```bash
databricks notebook run "<your-workspace-path>/Databrick-Foreach-pipline-config-driven/config_table/ddl"
```
> คำสั่งนี้รันทุก cell ใน notebook จากเครื่อง local ผ่าน CLI ไม่ต้องเปิด Databricks UI
> แทนที่ `<your-workspace-path>` ด้วย path ของคุณใน Databricks workspace เช่น `/Users/your-email@company.com`

### Deploy ด้วย DABs

```bash
# Validate
databricks bundle validate --target dev

# Deploy
databricks bundle deploy --target dev

# Run
 databricks bundle run --target dev
```

สำหรับ production:

```bash
databricks bundle deploy --target prod
databricks bundle run --target prod
```
> target `prod` จะ build `.whl` จาก `logic_packages/` อัตโนมัติ

### สถาปัตยกรรม Job Workflow

```
GetValue (อ่าน config_table)
   ├──> BrozneLayer (for_each: รัน Bronze ทุกไปป์ไลน์ขนาน)
   │       └──> SilverLayer (for_each: รัน Silver ทุกไปป์ไลน์ขนาน)
   │              ├──> SCD2 (ประมวลผล SCD Type 2)
   │              └──> cross_check (ตรวจสอบข้อมูล)
   │                     └──> GoldLayer (รวมข้อมูล Gold)
   └──> CheckValue (for_each: ตรวจสอบค่า config)
```

### ตัวอย่างการเพิ่มไปป์ไลน์ใหม่

ไม่ต้องแก้โค้ด เพียง insert แถวใหม่ลงใน `config_table`:

```sql
INSERT INTO session_life_hamham.session_life.config_table
VALUES (
  'new_pipeline',
  '/Volumes/session_life_hamham/session_life/manual_file_folder/new_data.csv',
  'true', ',',
  'session_life_hamham.session_life.new_table',
  MAP('id', 'int', 'name', 'string', 'amount', 'int'),
  ARRAY('id'),
  'overwrite',
  NULL, false
);
```
รัน Job ใหม่อีกครั้ง ระบบจะรองรับไปป์ไลน์ใหม่อัตโนมัติ