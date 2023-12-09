// Tab switching functionality
function openTab(evt, tabName) {
  var i, tabcontent, tablinks;
  tabcontent = document.getElementsByClassName("tabcontent");
  for (i = 0; i < tabcontent.length; i++) {
    tabcontent[i].style.display = "none";
  }
  tablinks = document.getElementsByClassName("tablinks");
  for (i = 0; i < tablinks.length; i++) {
    tablinks[i].className = tablinks[i].className.replace(" active", "");
  }
  document.getElementById(tabName).style.display = "block";
  evt.currentTarget.className += " active";

  // Load files if Download tab is opened
  if (tabName === "Download") {
    fetchFileListForDownload();
  }
}

// upload related code

// globals

let isPaused = false;
let fileQueue = [];
serverUrl = "http://localhost:5000/api/upload";

// functions

function setFileInputEnabled(enabled) {
  document.getElementById("fileInput").disabled = !enabled;
}

// Function to upload the next file in the queue
function uploadNextFile() {
  if (fileQueue.length === 0) {
    setFileInputEnabled(true); // Enable file input again when all files are processed
    return;
  }

  const file = fileQueue.shift(); // Get the next file from the queue
  currentFile = file;
  const fileUrl = URL.createObjectURL(file);

  chrome.runtime.sendMessage({
    type: "startUpload",
    fileUrl: fileUrl,
    fileName: file.name,
    fileSize: file.size,
  });
}

function resetProgressBar() {
  const progressBar = document.getElementById("progressBar");
  progressBar.style.width = "0%";
  progressBar.textContent = "0%";
}

function fetchFileList() {
  fetch("http://localhost:5000/api/upload")
    .then((response) => response.text())
    .then((data) => {
      document.getElementById("filesList").innerHTML = data;
    });
}

function updateProgressBar(progress) {
  if (progress == null || isNaN(progress)) {
    // Skip the update or set a default value if progress is null or not a number
    return;
  }
  const progressBar = document.getElementById("progressBar");
  progressBar.style.width = progress + "%";
  progressBar.textContent = progress.toFixed(0) + "%";
}

// Event Listeners (Document)

// Add event listeners for tab switching
document
  .getElementById("uploadTab")
  .addEventListener("click", function (event) {
    openTab(event, "Upload");
  });

// Modify the file input change event to handle multiple files
document.getElementById("fileInput").addEventListener("change", function () {
  if (this.files.length === 0) return;
  fileQueue = Array.from(this.files); // Store all selected files in the queue
  uploadNextFile(); // Start uploading the first file
});

document.getElementById("restartButton").addEventListener("click", function () {
  if (currentFile) {
    chrome.runtime.sendMessage({
      type: "restartUpload",
      file: {
        fileUrl: URL.createObjectURL(currentFile),
        name: currentFile.name,
        size: currentFile.size,
      },
    });
    setFileInputEnabled(false);
  }
});

document.addEventListener("DOMContentLoaded", function () {
  fetch("http://localhost:5000/api/upload")
    .then((response) => response.text())
    .then((data) => {
      document.getElementById("filesList").innerHTML = data;
    });
});

document
  .getElementById("filesList")
  .addEventListener("click", function (event) {
    if (event.target.classList.contains("delete-button")) {
      const fileName = event.target.getAttribute("data-filename");
      fetch(`http://localhost:5000/api/upload/delete/${fileName}`, {
        method: "DELETE",
      })
        .then((response) => {
          if (response.ok) {
            event.target.parentNode.remove(); // Remove the file item from the list
          }
        })
        .catch((error) => {
          console.error("Error deleting file:", error);
        });
    }
  });

document.getElementById("cancelButton").addEventListener("click", function () {
  chrome.runtime.sendMessage({
    type: "cancelCurrentUpload",
    fileName: currentFile.name,
  });
});

document
  .getElementById("pauseResumeButton")
  .addEventListener("click", function () {
    chrome.runtime.sendMessage({ type: "pauseResumeUpload" });
    this.textContent = isPaused ? "Pause" : "Resume";
    isPaused = !isPaused;
  });

// Call fetchFileList on DOMContentLoaded to load the initial file list
document.addEventListener("DOMContentLoaded", function () {
  fetchFileList();
  document.getElementsByClassName("tablinks")[0].click();
});

document
  .getElementById("deleteAllButton")
  .addEventListener("click", function () {
    chrome.runtime.sendMessage({ type: "deleteAllFiles" });
  });

// Event Listeners (Chrome)

// Listener for upload completion or cancellation message from background script
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "uploadComplete" || message.type === "uploadCancelled") {
    uploadNextFile(); // Trigger next file upload
  }
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "updateProgress") {
    updateProgressBar(message.progress);
  } else if (message.type === "resetProgress") {
    resetProgressBar();
  }
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "showProgressBar") {
    document.getElementById("progressBarContainer").style.display = "block";
  } else if (message.type === "hideProgressBar") {
    document.getElementById("progressBarContainer").style.display = "none";
  } else if (message.type === "refreshFileList") {
    fetchFileList();
  }
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "refreshFileList") {
    fetchFileList(); // Fetch and update the file list
  }
});

// download related code

// globals

let isDownloadPaused = false;
let isDownloading = false;
let downloadQueue = [];
let downloadAbortController = null;
let currentDownloadAbortController = null;
let downloadAborted = false;
let currentFile = null;
let currentFileName = null;
let currentChunkCount = null;
let currentChunkIndex = 0;
let currentChunks = [];

// functions

function assembleAndDownloadFile(chunks, fileName) {
  // Create a new blob from the array of chunks
  const fileBlob = new Blob(chunks);

  // Create a URL for the blob
  const url = window.URL.createObjectURL(fileBlob);

  // Create a temporary anchor element and trigger a download
  const a = document.createElement("a");
  a.href = url;
  a.download = fileName;
  document.body.appendChild(a);
  a.click();

  // Clean up by revoking the blob URL and removing the anchor element
  window.URL.revokeObjectURL(url);
  a.remove();
}

// Function to update the queue display
function updateDownloadQueueDisplay() {
  const queueDisplayElement = document.getElementById("downloadQueueDisplay");
  const queueCount = downloadQueue.length;

  if (queueCount > 0) {
    queueDisplayElement.textContent = `${queueCount} more file(s) in queue`;
  } else {
    queueDisplayElement.textContent = "No files in queue";
  }
}

// Function to download a file
function downloadFile(fileName, chunkCount) {
  // Reset download state for a new file
  currentFileName = fileName;
  currentChunkCount = chunkCount;
  currentChunkIndex = 0;
  currentChunks = [];

  downloadAborted = false; // Reset flag at the start of a new download

  // Show the download progress bar
  document.getElementById("downloadProgressBarContainer").style.display =
    "block";
  updateDownloadProgressBar(0);

  isDownloading = true; // Set the flag to true when download starts

  downloadAbortController = new AbortController(); // Create a new controller for each download
  const { signal } = downloadAbortController;

  currentDownloadAbortController = new AbortController();

  continueDownload();
}

function continueDownload() {
  if (
    currentChunkIndex < currentChunkCount &&
    !downloadAborted &&
    isDownloading &&
    !isDownloadPaused
  ) {
    fetch(
      `http://localhost:5000/api/upload/download/${currentFileName}/${currentChunkIndex}`,
      { signal: currentDownloadAbortController.signal }
    )
      .then((response) => response.blob())
      .then((blob) => {
        if (downloadAborted) {
          console.log("Download was aborted, stopping further processing.");
          return;
        }
        currentChunks.push(blob);
        currentChunkIndex++;
        updateDownloadProgressBar(
          (currentChunkIndex / currentChunkCount) * 100
        );

        if (currentChunkIndex < currentChunkCount) {
          continueDownload(); // Fetch the next chunk
        } else {
          // All chunks downloaded
          assembleAndDownloadFile(currentChunks, currentFileName);
          downloadQueue.shift();
          updateDownloadQueueDisplay();
          resetProgressBarAndHide();
          if (downloadQueue.length > 0) {
            startNextDownload();
          }
        }
      })
      .catch((error) => {
        if (error.name === "AbortError") {
          console.log("Download aborted: ", currentFileName);
        } else {
          console.error("Error downloading chunk: ", error);
        }
      });
  }
}

function resetProgressBarAndHide() {
  document.getElementById("downloadProgressBarContainer").style.display =
    "none";
  updateDownloadProgressBar(0);
}

function toggleDownloadPause() {
  isDownloadPaused = !isDownloadPaused;
  document.getElementById("pauseResumeDownloadButton").textContent =
    isDownloadPaused ? "Resume" : "Pause";
  if (!isDownloadPaused && isDownloading) {
    // Resume the download process
    continueDownload();
  }
}

function cancelCurrentDownload() {
  if (currentDownloadAbortController) {
    currentDownloadAbortController.abort();
    downloadAborted = true;
    currentDownloadAbortController = null;

    isDownloading = false;
    updateDownloadProgressBar(0);
    if (downloadQueue.length > 0) {
      downloadQueue.shift(); // Remove the cancelled file from the queue
      updateDownloadQueueDisplay();
      startNextDownload(); // Immediately start next download
    } else {
      resetDownloadState();
    }
  }
}

function resetDownloadState() {
  document.getElementById("downloadProgressBarContainer").style.display =
    "none";
  updateDownloadProgressBar(0);
  isDownloading = false; // Ensure downloading flag is reset
  downloadQueue = []; // Clear the download queue
  updateDownloadQueueDisplay();
}


function updateDownloadProgressBar(progress) {
  const progressBar = document.getElementById("downloadProgressBar");
  progressBar.style.width = progress + "%";
  progressBar.textContent = progress.toFixed(0) + "%";
}

// Function to fetch and display available files for download
function fetchFileListForDownload() {
  fetch("http://localhost:5000/api/uploadfiles")
    .then((response) => response.text())
    .then((data) => {
      document.getElementById("filesDownloadList").innerHTML = data;
    });
}

function initiateFileDownload(fileName) {
  fetch(`http://localhost:5000/api/upload/metadata/${fileName}`)
    .then((response) => response.json())
    .then((metadata) => {
      downloadQueue.push({ fileName, chunkCount: metadata.chunkCount });
      updateDownloadQueueDisplay(); // Update the queue display
      if (downloadQueue.length === 1) {
        startNextDownload();
      }
    })
    .catch((error) => {
      console.error("Error fetching file metadata:", error);
    });
}

function startNextDownload() {
  if (downloadQueue.length > 0) {
    const { fileName, chunkCount } = downloadQueue[0];
    downloadFile(fileName, chunkCount);
  } else {
    resetDownloadState(); // Reset state if no more files to download
  }
}

function restartCurrentDownload() {
  if (currentDownloadAbortController) {
    currentDownloadAbortController.abort(); // Abort the current download
    downloadAborted = true;
    currentDownloadAbortController = null;

    if (isDownloading) {
      isDownloading = false; // Reset downloading flag
      updateDownloadProgressBar(0); // Reset progress bar

      if (downloadQueue.length > 0) {
        const currentDownload = downloadQueue.shift(); // Remove and get the current download details
        downloadQueue.unshift(currentDownload); // Add it back to the start of the queue
        startNextDownload(); // Immediately start the download
      }
    }
  }
}

// Event Listeners (Document)

document
  .getElementById("downloadTab")
  .addEventListener("click", function (event) {
    openTab(event, "Download");
  });

document
  .getElementById("pauseResumeDownloadButton")
  .addEventListener("click", function () {
    toggleDownloadPause();
  });

document
  .getElementById("cancelCurrentDownloadButton")
  .addEventListener("click", cancelCurrentDownload);

document
  .getElementById("cancelDownloadButton")
  .addEventListener("click", function () {
    if (downloadAbortController) {
      downloadAbortController.abort(); // Abort all downloads
      downloadAbortController = null;
    }
    resetDownloadState();
    downloadQueue = []; // Clear the queue
    updateDownloadQueueDisplay();
  });

document
  .getElementById("filesDownloadList")
  .addEventListener("click", function (event) {
    if (event.target.classList.contains("download-button")) {
      const fileName = event.target.getAttribute("data-filename");
      initiateFileDownload(fileName);
    }
  });

document
  .getElementById("restartDownloadButton")
  .addEventListener("click", restartCurrentDownload);
