// worker.js

// Function to perform the fetch operation
const fetchData = async (url) => {
    try {
        const headers = {
            'Authorization': 'Bearer eyJhbGciOiJIUzI1NiJ9.eyJyZXBvSWQiOiI2Nzg1OTI2NDQ1YTU3NTIyMWRjOTFmNzciLCJ1c2VySWQiOiI2NGMzYjgyYmZhZmExNmI1MTQyNTNmZDgiLCJhY2Nlc3MiOiJSRUFEIiwidXNlUHJpdmF0ZUxpbmsiOmZhbHNlLCJpYXQiOjE3NDY0OTE0NTQsImV4cCI6MTc0NjQ5MjM1NCwiaXNzIjoiaHR0cHM6Ly9odWdnaW5nZmFjZS5jbyJ9.ncVN6pCyb-PnheWgCAx60DCUXGmlP3hIgwO4B5y5Nns'
        };
        const response = await fetch(url, { headers: headers });
        if (!response.ok) {
            // Handle HTTP errors (e.g., 404, 500)
            throw new Error(`HTTP error! Status: ${response.status}`);
        }
        const text = await response.text(); // Get the response as text
        return text;
    } catch (error) {
        // Handle network errors, fetch API errors, and other exceptions
        return { error: error.message };
    }
};

// Listen for messages from the main script
self.onmessage = async function (event) {
    const url = event.data.url;
    const result = await fetchData(url);
    // Post the result back to the main script
    self.postMessage(result);
};
