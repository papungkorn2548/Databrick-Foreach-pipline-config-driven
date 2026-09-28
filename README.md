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

## ขั้นตอนการใช้งานทั้งหมด (Step-by-Step Guide)

### ขั้นตอนที่ 1 — Clone โปรเจกต์

```bash
git clone <repository-url>
cd Databrick-Foreach-pipline-config-driven
```

### ขั้นตอนที่ 2 — ติดตั้ง Databricks CLI (ถ้ายังไม่มี)

```bash
pip install databricks-cli
databricks configure
```

> สำหรับ Databricks CLI เวอร์ชันใหม่ (v0.200+) ให้ใช้ `pip install databricks-sdk` และ `databricks auth login` แทน

### ขั้นตอนที่ 3 — Deploy Bundle เพื่อนำ Notebook ขึ้น Workspace

ก่อนรัน DDL notebook ได้ ต้อง deploy bundle ก่อนเพื่อให้ notebook และไฟล์ CSV ถูกคัดลอกขึ้น Workspace:

```bash
# Validate bundle config
databricks bundle validate --target dev

# Deploy bundle ขึ้น Workspace
databricks bundle deploy --target dev
```

หลัง deploy สำเร็จ notebook และไฟล์ CSV ใน `data_set/` จะถูกคัดลอกไปยัง:

```
/Workspace/Users/<your-email>/.bundle/CF_DRIVEN_DATABIRCK/dev/
├── notebooks/   (notebook ทั้งหมด)
└── files/
    └── data_set/   (ไฟล์ CSV ตัวอย่าง)
```

### ขั้นตอนที่ 4 — รัน DDL Notebook (สร้าง Catalog, Schema, Volume, Config Table)

รัน notebook `config_table/ddl` เพื่อสร้าง:

* Catalog: `session_life_hamham`
* Schema: `session_life`
* Volume: `manual_file_folder` (สำหรับเก็บไฟล์ CSV)
* Table: `config_table` (ตาราง config หลัก)
* คัดลอกไฟล์ CSV จาก `data_set/` ไปยัง Volume อัตโนมัติ
* Insert ข้อมูล config ของทุกไปป์ไลน์ตัวอย่าง (orders, customers, products, ...) ลงใน `config_table`

**วิธีที่ 1 — รันจาก Databricks UI (ง่ายสุด)**

1. เปิด notebook `config_table/ddl` ใน Databricks (จาก path ที่ deploy ไว้ในขั้นตอนที่ 3)
2. กด **Run All**
3. รอจนกว่าทุก cell จะรันเสร็จ

**วิธีที่ 2 — รันผ่าน CLI**

```bash
databricks jobs create --json '{
  "name": "run-ddl",
  "tasks": [{
    "task_key": "ddl",
    "notebook_task": {
      "notebook_path": "<your-workspace-path>/.bundle/CF_DRIVEN_DATABIRCK/dev/notebooks/ddl",
      "source": "WORKSPACE"
    }
  }]}'

databricks jobs run-now --job-id <job-id>
```

> แทนที่ `<your-workspace-path>` ด้วย path ของคุณ เช่น `/Workspace/Users/your-email@company.com`

### ขั้นตอนที่ 5 — ตรวจสอบ Config Table

หลังจากรัน DDL notebook แล้ว ตรวจสอบว่า `config_table` มีข้อมูลครบถ้วน:

```sql
SELECT * FROM session_life_hamham.session_life.config_table;
```

แต่ละแถวใน `config_table` ประกอบด้วย:

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

### ขั้นตอนที่ 6 — ตรวจสอบไฟล์ CSV ใน Volume

ตรวจสอบว่าไฟล์ CSV ถูกคัดลอกไปยัง Volume แล้ว:

```python
dbutils.fs.ls("/Volumes/session_life_hamham/session_life/manual_file_folder/")
```

ควรเห็นไฟล์ CSV ทั้งหมด เช่น `orders.csv`, `customers.csv`, `products.csv`, `payments.csv`, `returns.csv`, `shipments.csv`, `stores.csv`, `suppliers.csv`, `categories.csv`, `promotions.csv`, `order_items.csv`, `employee_scd2.csv`, `employee_source_silver.csv`

### ขั้นตอนที่ 7 — Deploy Job Pipeline ด้วย DABs

หลังจาก DDL และ config พร้อมแล้ว ให้ deploy Job ที่จะรัน pipeline:

```bash
# Validate
databricks bundle validate --target dev

# Deploy
databricks bundle deploy --target dev
```

สำหรับ production:

```bash
databricks bundle validate --target prod
databricks bundle deploy --target prod
```

> target `prod` จะ build `.whl` จาก `logic_packages/` อัตโนมัติ ก่อน deploy

### ขั้นตอนที่ 8 — รัน Pipeline

```bash
# รัน pipeline สำหรับ dev
databricks bundle run --target dev

# รัน pipeline สำหรับ prod
databricks bundle run --target prod
```

หรือรันจาก Databricks UI → ไปที่ **Jobs** → เลือก Job ที่ deploy ไว้ → กด **Run Now**

### ขั้นตอนที่ 9 — ติดตามผลการรัน (Monitoring)

1. ไปที่ **Jobs** ใน Databricks UI
2. เลือก Job ที่รัน → ดู **Run details**
3. ตรวจสอบสถานะของแต่ละ task:
   * **GetValue** — อ่าน config_table → ส่งค่าไปยัง for_each
   * **BronzeLayer** — รัน Bronze ทุกไปป์ไลน์ขนาน (for_each)
   * **SilverLayer** — รัน Silver ทุกไปป์ไลน์ขนาน (for_each) (DQ + SCD2)
   * **SCD2** — ประมวลผล SCD Type 2
   * **cross_check** — ตรวจสอบข้อมูล Silver
   * **GoldLayer** — รวมข้อมูล Gold
   * **CheckValue** — ตรวจสอบค่า config (for_each)
4. หาก task ใดมี error ให้คลิกเข้าไปดู log และ stack trace

### สถาปัตยกรรม Job Workflow

```
GetValue (อ่าน config_table)
   ├──> BronzeLayer (for_each: รัน Bronze ทุกไปป์ไลน์ขนาน)
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