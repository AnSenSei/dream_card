// Import the functions you need from the SDKs you need
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import { getAuth } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js";
// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyA0Fcw9CocUjxB75dYQMKfk5WJ05urcA-4",
  authDomain: "chouka-e474a.firebaseapp.com",
  projectId: "chouka-e474a",
  storageBucket: "chouka-e474a.appspot.com",
  messagingSenderId: "153362894868",
  appId: "1:153362894868:web:a0a0a924e56722671e1701",
  measurementId: "G-C1Q68FEN7Q"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
// Initialize Firebase Authentication and get a reference to the service
const auth = getAuth(app);

// Export auth to be used in other scripts
export { auth }; 