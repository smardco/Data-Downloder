# GFS Weather Data Downloader

A powerful and configurable Python script designed to automatically download weather forecast data from the **NOAA GFS (Global Forecast System) 0.25-degree model**.

This tool automates the entire process, from finding the latest available data to downloading multiple files concurrently and verifying the output.

---

## Key Features

* **Automatic Cycle Detection**: Intelligently finds the latest available GFS data cycle (00Z, 06Z, 12Z, 18Z) ready for download.
* **Highly Configurable**: Easily control everything from a single `config.yaml` file—no code changes needed!
    * Define your target geographical region (bounding box).
    * Select specific weather variables (e.g., Temperature, Wind, Humidity).
    * Choose desired atmospheric levels (e.g., surface, 850mb, 500mb).
* **Concurrent Downloads**: Uses multi-threading to fetch multiple files at once, significantly speeding up the process.
* **Data Verification**: Automatically checks for missing or empty files after the download process is complete.
* **Detailed Logging**: Records all actions, successes, and errors into a log file for easy monitoring and debugging.

---

##  Getting Started

Follow these steps to get the downloader up and running.

### Prerequisites

* Python 3.8 or newer
* Git

### Installation

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/smardco/Data-Downloader.git](https://github.com/smardco/Data-Downloader.git)
    ```

2.  **Navigate to the project directory:**
    ```bash
    cd Data-Downloader
    ```

3.  **Install the required libraries:**
    ```bash
    pip install -r requirements.txt
    ```

### Configuration

1.  **Create your configuration file.** A template is provided in this repository.
    *(On your computer, you can just rename `config.yaml` if needed, but this is a good practice).*

2.  **Edit `config.yaml`** to match your needs:
    * Set the geographical `region` (toplat, bottomlat, leftlon, rightlon).
    * Update the `variables` and `level` lists with the parameters you want to download.
    * Change `base_folder` to your desired output directory.

### Usage

Once configured, run the script from your terminal:

```bash
python download_script.py
```

The script will start the download process, and you can monitor its progress in the terminal and the log file. Your data will be saved in the directory specified in your configuration.

---
