// Global variables
const ENDPOINTS = {
    GET_ALL_COLLECTIONS: 'http://localhost:8080/gacha/api/v1/storage/collection-metadata',
    ADD_COLLECTION: 'http://localhost:8080/gacha/api/v1/storage/collection-metadata',
};

let collectionModal = null;

// DOM Content Loaded
document.addEventListener('DOMContentLoaded', function() {
    // Initialize Bootstrap modal
    collectionModal = new bootstrap.Modal(document.getElementById('collection-modal'));
    
    // Function to update sort parameters and refresh
    function updateSortParams(sortBy, sortOrder) {
        const params = new URLSearchParams(window.location.search);
        
        // Update sort parameters
        if (sortBy) params.set('sort_by', sortBy);
        if (sortOrder) params.set('sort_order', sortOrder);
        
        // Create the new URL with updated parameters
        const newUrl = `${window.location.pathname}?${params.toString()}`;
        history.pushState({}, '', newUrl);
        
        // Refresh the data with new parameters
        fetchAllCollections();
    }
    
    // Initial setup
    fetchAllCollections();
    
    // Set up event listeners
    document.getElementById('create-collection-btn').addEventListener('click', showAddCollectionModal);
    document.getElementById('collection-form').addEventListener('submit', handleCollectionSubmit);
});

// Fetch all collections
async function fetchAllCollections() {
    try {
        // Add explicit sort parameters to ensure consistent sorting behavior
        const url = `${ENDPOINTS.GET_ALL_COLLECTIONS}?sort_by=card_name&sort_order=desc`;
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error('Failed to fetch collections');
        }
        
        const collections = await response.json();
        displayCollections(collections);
    } catch (error) {
        console.error('Error fetching collections:', error);
        displayError('Failed to load collections. Please try again.');
    }
}

// Display collections in the collections table
function displayCollections(collections) {
    const collectionsTableBody = document.getElementById('collections-table-body');
    if (!collectionsTableBody) return;
    
    collectionsTableBody.innerHTML = '';
    
    if (!collections || collections.length === 0) {
        const row = document.createElement('tr');
        const cell = document.createElement('td');
        cell.colSpan = 4;
        cell.textContent = 'No collections found. Create a new collection to get started.';
        cell.style.textAlign = 'center';
        row.appendChild(cell);
        collectionsTableBody.appendChild(row);
        return;
    }
    
    collections.forEach(collection => {
        const row = document.createElement('tr');
        
        // Collection Name
        const nameCell = document.createElement('td');
        nameCell.textContent = collection.name;
        row.appendChild(nameCell);
        
        // Firestore Collection
        const firestoreCollectionCell = document.createElement('td');
        firestoreCollectionCell.textContent = collection.firestoreCollection;
        row.appendChild(firestoreCollectionCell);
        
        // Storage Prefix
        const storagePrefixCell = document.createElement('td');
        storagePrefixCell.textContent = collection.storagePrefix;
        row.appendChild(storagePrefixCell);
        
        // Actions column
        const actionsCell = document.createElement('td');
        const viewBtn = document.createElement('button');
        viewBtn.textContent = 'View Cards';
        viewBtn.classList.add('btn', 'btn-sm', 'btn-info', 'me-2');
        viewBtn.addEventListener('click', () => {
            window.location.href = `view_cards.html?collectionName=${collection.name}`;
        });
        actionsCell.appendChild(viewBtn);
        
        const uploadBtn = document.createElement('button');
        uploadBtn.textContent = 'Upload Card';
        uploadBtn.classList.add('btn', 'btn-sm', 'btn-primary');
        uploadBtn.addEventListener('click', () => {
            window.location.href = `index.html?collection_metadata_id=${collection.name}`;
        });
        actionsCell.appendChild(uploadBtn);
        
        row.appendChild(actionsCell);
        
        collectionsTableBody.appendChild(row);
    });
}

// Show modal for adding a new collection
function showAddCollectionModal() {
    // Reset form
    document.getElementById('collection-form').reset();
    document.getElementById('collectionModalLabel').textContent = 'Add Collection Metadata';
    
    // Show modal
    collectionModal.show();
}

// Parse URL query parameters
function getUrlParams() {
    const params = new URLSearchParams(window.location.search);
    return {
        collectionName: params.get('collectionName'),
        sort_by: params.get('sort_by') || 'point_worth',
        sort_order: params.get('sort_order') || 'desc'
    };
}

// Function to navigate to a collection view
function viewCollection(collectionName) {
    if (!collectionName) return;
    
    // Redirect to the view_cards.html page with the collection name as a parameter
    window.location.href = `/admin_frontend/view_cards.html?collectionName=${encodeURIComponent(collectionName)}`;
}

// Handle collection form submission
async function handleCollectionSubmit(event) {
    event.preventDefault();
    
    const form = event.target;
    const formData = new FormData(form);
    
    // Convert FormData to JSON
    const collectionData = {
        name: formData.get('name'),
        firestoreCollection: formData.get('firestoreCollection'),
        storagePrefix: formData.get('storagePrefix')
    };
    
    // Add function to navigate to a specific collection view
    function viewCollection(collectionName) {
        if (!collectionName) return;
        window.location.href = `/view_cards.html?collectionName=${encodeURIComponent(collectionName)}`;
    }
    
    try {
        const response = await fetch(ENDPOINTS.ADD_COLLECTION, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(collectionData)
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || 'Failed to add collection');
        }
        
        // Hide modal
        collectionModal.hide();
        
        // Refresh collections list
        fetchAllCollections();
        
        // Show success message
        displaySuccess('Collection added successfully!');
    } catch (error) {
        console.error('Error adding collection:', error);
        displayError(`Failed to add collection: ${error.message}`);
    }
}

// Display error message
function displayError(message) {
    displayAlert(message, 'danger');
}

// Display success message
function displaySuccess(message) {
    displayAlert(message, 'success');
}

// Display alert
function displayAlert(message, type) {
    const alertsContainer = document.getElementById('alerts-container');
    if (!alertsContainer) return;
    
    const alert = document.createElement('div');
    alert.className = `alert alert-${type} alert-dismissible fade show`;
    alert.role = 'alert';
    
    alert.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    alertsContainer.appendChild(alert);
    
    // Auto-dismiss after 5 seconds
    setTimeout(() => {
        if (alert.parentNode) {
            const bsAlert = new bootstrap.Alert(alert);
            bsAlert.close();
        }
    }, 5000);
}