# 🕵️ Job Crawler & Scraper API

<p align="center">
  <a href="https://krisgarg.in" target="_blank">
    <img src="https://img.shields.io/badge/Portfolio-krisgarg.in-000000?style=for-the-badge&logo=google-chrome&logoColor=white" />
  </a>
</p>

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-v0.100+-green.svg)](https://fastapi.tiangolo.com/)
[![MongoDB](https://img.shields.io/badge/MongoDB-Atlas-lightgrey.svg)](https://www.mongodb.com/cloud/atlas)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A high-performance hybrid job scraper designed to collect job listings from major aggregators (Indeed, LinkedIn, Naukri) and direct company career pages. Optimized for deployment on **Render**.

---

## 🚀 Quick Start (Local Setup)

1. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Run the API Server**

   ```bash
   uvicorn server:app --reload
   ```

---

## ☁️ Deployment on Render

This project is pre-configured for seamless deployment on Render.

### **Build & Start Commands**

Use these commands in your Render service configuration:

* **Build Command:**

  ```bash
  pip install -r requirements.txt
  ```

* **Start Command:**

  ```bash
  uvicorn server:app --host 0.0.0.0 --port $PORT
  ```

### **Required Environment Variables**

Configure these in the Render "Environment" tab:

* `MongoDB_URL`: Your MongoDB Atlas connection string.
* `APP_URL`: The URL of your deployed Render service (e.g., `https://your-app.onrender.com`).

---

## 📂 Project Structure

* **`server.py`**: 🚀 **Main Entry Point**. FastAPI server that handles job scheduling, keep-alive pings, and the scraping API.
* **`company_careers.py`**: Specialized scrapers for direct company APIs (Amazon, Flipkart, etc.).
* **`jobspy/`**: Integrated library for scraping major job aggregators.
* **`requirements.txt`**: List of Python dependencies.

---

## 🌐 API Endpoints

| Endpoint  | Method | Description                                       |
| :-------- | :----- | :------------------------------------------------ |
| `/`       | `GET`  | Health check & welcome message.                   |
| `/scrape` | `POST` | Trigger a new scraping task (Background process). |
| `/status` | `GET`  | Check the current scraping status.                |
| `/health` | `GET`  | Endpoint used for keep-alive pings.               |

---

## 🛠️ Tech Stack & Dependencies

### **Core Engines**

* **[jobspy](https://github.com/Bunsly/JobSpy)**: Scrapes **Indeed**, **LinkedIn**, and **Naukri**.
* **HTTPX/Requests**: High-speed calls to direct company career APIs.

### **Key Libraries**

* **FastAPI**: Modern, fast web framework for building APIs.
* **APScheduler**: Handles automated scraping every 12 hours.
* **PyMongo**: Secure connection to MongoDB Atlas for job storage.
* **Pandas**: Used internally for data structuring and cleaning.

---

## 🦾 Automation Features

1. **Auto-Scraping**: Automatically scrapes for new jobs every **12 hours**.
2. **Keep-Alive**: Built-in pinging mechanism to prevent Render's free tier from spinning down.
3. **Regex Cleaning**: Powerful salary extraction (LPA/CTC formats) and HTML tag removal.
4. **Parallel Execution**: Uses `concurrent.futures` to scrape multiple company sites simultaneously.

---

## 📄 License

Distributed under the MIT License. See `LICENSE` (if available) for more information.
