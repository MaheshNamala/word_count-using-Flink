📁 Project Structure


flink\_wordcount\_project/
├── input/
│   ├── sample.txt                # Input for batch\_wordcount.py
│   └── stream\_input.csv          # Input for streaming\_wordcount.py
├── batch\_wordcount.py            # Batch mode with UDTF
├── streaming\_wordcount.py        # Simulated streaming using CSV
├── tumbling\_window\_wordcount.py  # (Optional) Windowed stream

⚙️ Setup Instructions

✅ Step 1: Create Environment & Install PyFlink

```bash
python -m venv flink_env
flink_env\Scripts\activate          # On Windows
pip install pyflink
````


## ▶️ How to Run

### 📝 1. Batch Word Count

**Input:** `input/sample.txt`

Example:

```
hello world
flink is awesome
hello flink
```

**Run:**

```bash
python batch_wordcount.py
```

**Output:**

```
+I[awesome, 1]
+I[flink, 2]
+I[hello, 2]
+I[is, 1]
+I[world, 1]
```

📸 ![Batch Output](./Screenshot%20\(3\).png)

---

### 🧪 2. Simulated Streaming Word Count

**Input:** `input/stream_input.csv`

Start with:

```
hello
hello
world
```

**Run:**

```bash
python streaming_wordcount.py
```

**Then simulate stream by appending:**

```bash
echo flink >> input/stream_input.csv
```

**Output Example:**

```
+I[hello, 2]
+I[world, 1]
+I[flink, 1]

## 📸 Screenshots

| Description             | Screenshot                             |
| ----------------------- | -------------------------------------- |
| Input file (sample.txt) | ![Input](./text file\).png)     |
| Batch Mode Output       | ![Batch](./code\).png)     |
| Streaming Output        | ![Streaming](./output in terminal\).png) |

---

## 🔥 Features Covered

* PyFlink batch and streaming APIs
* UDTF with Python for word splitting
* Group-by aggregation
* Filesystem + CSV ingestion
* Print sink for quick display

---

## 💡 Notes

* No Java or Flink cluster is required
* Built and tested using:

  * Python 3.10
  * PyFlink via pip
  * VS Code + CMD on Windows 10
