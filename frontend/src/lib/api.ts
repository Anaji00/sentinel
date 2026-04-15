/**
 * @file This file configures and exports a centralized Axios instance for making API requests.
 * It helps in keeping the API communication logic clean, consistent, and easy to manage.
 */

import axios from "axios";

// Pulls the API URL and Key from Next.js environment variables.
// These variables should be defined in a `.env.local` file at the root of the project.
// The `NEXT_PUBLIC_` prefix is required to expose these variables to the browser.
// A default value is provided for local development if the environment variable is not set.
const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000/api/v1';
const API_KEY = process.env.NEXT_PUBLIC_API_KEY || '';

/**
 * A pre-configured Axios instance for making requests to our backend API.
 *
 * Using a single, centralized instance ensures that all requests share the same base configuration,
 * such as the base URL and common headers (like Content-Type and the API key).
 * This avoids repetition and makes the code easier to maintain.
 */
export const apiClient = axios.create({
    baseURL: API_BASE_URL,
    headers: {
        'Content-Type': 'application/json',
        'X-API-KEY': API_KEY,
    },
});

/**
 * A simple data fetcher function, often used with data-fetching libraries like SWR or React Query.
 * It takes a URL, makes a GET request using our pre-configured `apiClient`, and returns the response data.
 *
 * @param {string} url - The API endpoint to fetch data from (e.g., '/users').
 * @returns {Promise<any>} A promise that resolves to the JSON data from the API response.
 */
export const fetcher = (url: string) => apiClient.get(url).then((res) => res.data);