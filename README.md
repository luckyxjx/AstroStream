# 🌌 AstroStream

AstroStream is a containerized data pipeline that fetches astronomical data from NASA APIs, processes it using Apache Spark, and visualizes it on an interactive dashboard using Dash.

---

## 📁 Project Overview

AstroStream allows users to:

* Retrieve real-time astronomical data.
* Process data with Spark in a distributed pipeline.
* View visualized insights through a web-based dashboard.

---

## 🛆 Virtual Environment Setup (For Local Python Testing)

> Optional: Use this if you plan to test or develop Python scripts locally before using Docker.

### macOS/Linux

```bash
python3 -m venv venv
source venv/bin/activate
```

### Windows (PowerShell)

```powershell
python -m venv venv
venv\Scripts\Activate.ps1
```

---

## 🧪 Build and Run Instructions

### 1. 🔑 Verify API Key

Open the `docker-compose.yml` file and **replace**:

```yaml
YOUR_NASA_API_KEY
```

with your actual [NASA API key](https://api.nasa.gov/).

---

### 2. 📂 Open Terminal in Project Directory

```bash
cd path/to/astrostream
```

---

### 3. 🏗️ Build Docker Images

```bash
docker-compose build
```

---

### 4. 🚀 Start Services

```bash
docker-compose up -d
```

---

### 5. ✅ Check Container Status

```bash
docker-compose ps
```

---

### 6. 🐛 Monitor Logs (If Needed)

```bash
docker-compose logs -f dashboard
docker-compose logs -f fetcher
docker-compose logs spark-job-runner
# etc.
```

---

## 🌐 Accessing Services

* **Spark UI:** [http://localhost:8080](http://localhost:8080)
* **AstroStream Dashboard:** [http://localhost:8050](http://localhost:8050)
  *(Give the system a moment to populate data.)*

---

## 💪 Stopping the Application

### 1. ❌ Stop and Remove Containers

```bash
docker-compose down
```

### 2. 🧹 (Optional) Remove Data Volumes

```bash
docker-compose down -v
```

---

## 🔧 Fixing Dashboard Obsolete Error

### If you encounter `ObsoleteAttributeException`, apply this fix:

1. Open `dashboard/app.py` and ensure the **last line** reads:

```python
app.run(...)  # instead of app.run_server(...)
```

2. Rebuild only the dashboard container:

```bash
docker-compose build dashboard
```

3. Restart services:

```bash
docker-compose down
docker-compose up -d
```

4. Check dashboard logs again:

```bash
docker-compose logs dashboard
```

---

## 🛠️ Tech Stack

* **Python**
* **Dash** (for dashboard UI)
* **Apache Spark** (data processing)
* **Docker & Docker Compose** (container orchestration)
* **NASA API** (data source)

---

## 📄 License

MIT License. See `LICENSE` file for details.

---

## ✨ Contributions

Feel free to fork, open issues, or submit PRs
