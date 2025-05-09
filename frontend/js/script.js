// Import Firebase auth and necessary functions
import { auth } from './firebase-init.js';
import { onAuthStateChanged, signOut } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js";

console.log("Frontend JavaScript loaded.");

function navigateTo(page) {
    window.location.href = page;
}

// Function to handle user sign out
async function handleSignOut() {
    try {
        await signOut(auth);
        console.log("User signed out successfully.");
        window.location.href = 'login.html'; // Redirect to login page after sign out
    } catch (error) {
        console.error("Sign out error:", error);
        alert("Error signing out: " + error.message);
    }
}

// Function to update UI based on auth state
function updateNavUI(user) {
    const navLinks = document.querySelector('nav'); // Assuming your nav links are inside a <nav> tag
    if (!navLinks) return;

    // Remove existing auth-related links to prevent duplication
    const existingAuthLink = document.getElementById('auth-link');
    if (existingAuthLink) {
        existingAuthLink.remove();
    }
    const existingUserInfo = document.getElementById('user-info');
    if (existingUserInfo) {
        existingUserInfo.remove();
    }

    if (user) {
        // User is signed in
        const userInfo = document.createElement('span');
        userInfo.id = 'user-info';
        userInfo.textContent = `Logged in as: ${user.email}`;
        userInfo.style.marginRight = '10px'; // Basic styling

        const logoutLink = document.createElement('a');
        logoutLink.id = 'auth-link';
        logoutLink.href = '#';
        logoutLink.textContent = 'Logout';
        logoutLink.addEventListener('click', (e) => {
            e.preventDefault();
            handleSignOut();
        });
        
        navLinks.appendChild(userInfo); // Add user info display
        navLinks.appendChild(logoutLink); // Add logout link
    } else {
        // User is signed out
        const loginLink = document.createElement('a');
        loginLink.id = 'auth-link';
        loginLink.href = 'login.html';
        loginLink.textContent = 'Login/Sign Up';
        navLinks.appendChild(loginLink); // Add login link
    }
}


// Check auth state and protect pages
const protectedPages = ['draw.html', 'collection.html']; // Add other pages that need protection
const currentPage = window.location.pathname.split('/').pop();

onAuthStateChanged(auth, (user) => {
    updateNavUI(user); // Update navigation UI on all pages

    if (!user && protectedPages.includes(currentPage)) {
        console.log("User not authenticated. Redirecting to login page.");
        window.location.href = 'login.html';
    } else if (user && currentPage === 'login.html') {
        // If user is logged in and on login page, redirect to home
        window.location.href = 'index.html';
    }
    // If user is authenticated or on a public page, do nothing here
    // Specific page logic can proceed
});

// Expose navigateTo to global scope if it's called by inline HTML event attributes
window.navigateTo = navigateTo;

// Future interactive elements can be added here.
// For example, fetching pack data or card data from the backend API. 