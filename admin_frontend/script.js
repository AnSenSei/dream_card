document.addEventListener('DOMContentLoaded', () => {
    // Initialize variables
    const uploadForm = document.getElementById('uploadForm');
    const messageDiv = document.getElementById('message');
    const collectionDropdown = document.getElementById('collection_metadata_id');
    let fetchedCollections = []; // Store fetched collections for later use

    // Fetch collections for dropdown
    fetch('/gacha/api/v1/storage/collection-metadata')
        .then(response => response.json())
        .then(collections => {
            console.log('Fetched collections:', collections);
            fetchedCollections = collections; // Store for later use

            // Clear current options except the default one
            while (collectionDropdown.options.length > 1) {
                collectionDropdown.remove(1);
            }

            // Add each collection to the dropdown
            collections.forEach(collection => {
                const option = document.createElement('option');
                option.value = collection.name;
                option.textContent = collection.name;
                collectionDropdown.appendChild(option);
                console.log(`Added option: ${collection.name} with value: ${collection.name}`);
            });

            console.log('Collection dropdown populated with options. Current value:', collectionDropdown.value);

            // Add event listener to the dropdown to debug selection changes
            collectionDropdown.addEventListener('change', function() {
                console.log('Collection dropdown value changed to:', this.value);
            });

            // Set a default collection if none selected (for testing)
            if (collections.length > 0 && !collectionDropdown.value) {
                console.log("Setting a default collection for testing:", collections[0].name);
                collectionDropdown.value = collections[0].name;

                // Trigger the change event
                const event = new Event('change');
                collectionDropdown.dispatchEvent(event);
            }
        })
        .catch(error => {
            console.error('Error fetching collections:', error);
        });

    // Handle form submission
    if (uploadForm) {
        // Additional check to ensure collection is properly set
        uploadForm.addEventListener('submit', function(e) {
            e.preventDefault();

            // Show loading message
            messageDiv.innerHTML = '<p class="info">Uploading card...</p>';

            // Ensure the collection is properly included by setting both fields
            const selectedCollection = collectionDropdown.value;
            document.getElementById('collection_backup').value = selectedCollection;

            console.log('Form submitted. Collection dropdown value:', selectedCollection);
            console.log('Collection backup field value:', document.getElementById('collection_backup').value);

            // Create a new FormData object directly from the form
            const formData = new FormData(uploadForm);

            // Explicitly add collection_metadata_id to ensure it's included
            if (collectionDropdown.value) {
                console.log("Adding collection_metadata_id:", collectionDropdown.value);
                // Remove any existing value first
                formData.delete('collection_metadata_id');
                // Add as form field, not as URL parameter
                formData.append('collection_metadata_id', collectionDropdown.value);
                console.log("Added collection_metadata_id to form data");
            } else {
                console.warn("No collection selected, will use default collection");
            }

            // Debug FormData contents before submission
            console.log("Form data contents before submission:");
            for (let pair of formData.entries()) {
                console.log(`${pair[0]}: ${pair[1]}`);
            }

            // Make the API request with ONLY form data, no URL parameters
            // Use the backendUrl variable defined at the top of the script
            console.log("Submitting to URL:", backendUrl);
            
            fetch(backendUrl, {
                method: 'POST',
                body: formData,
                // Do not set Content-Type header - browser sets it with boundary
                // Don't set Content-Type header - browser sets it automatically with boundary
            })
            .then(response => {
                console.log("Response status:", response.status);
                if (!response.ok) {
                    if (response.status === 405) {
                        throw new Error('Method Not Allowed: This endpoint only accepts POST requests');
                    }
                    return response.json().then(errData => {
                        throw new Error(errData.detail || 'Upload failed');
                    }).catch(err => {
                        // In case the response doesn't have a valid JSON body
                        throw new Error(`Upload failed with status ${response.status}`);
                    });
                }
                return response.json();
            })
            .then(data => {
                console.log('Card uploaded successfully:', data);
                messageDiv.innerHTML = '<p class="success">Card uploaded successfully!</p>';
                uploadForm.reset();
            })
            .catch(error => {
                console.error('Error uploading card:', error);
                messageDiv.innerHTML = `<p class="error">Error: ${error.message}</p>`;
            });
        });
    }

    // Use the full backend URL including the scheme, host, port, and full path
    const backendUrl = 'http://localhost:8080/gacha/api/v1/storage/upload_card';
    const collectionsUrl = 'http://localhost:8080/gacha/api/v1/storage/collection-metadata';

    // Check URL parameters for collection_metadata_id
    const urlParams = new URLSearchParams(window.location.search);
    const collectionMetadataIdFromUrl = urlParams.get('collection_metadata_id');

    // Fetch collection metadata and populate dropdown
    fetchCollections(collectionMetadataIdFromUrl);

    // Function to fetch collection metadata and populate dropdown
    async function fetchCollections(selectedCollectionId = null) {
        try {
                // Update URL to use collection-metadata endpoint instead of collections
                const collectionMetadataUrl = collectionsUrl.replace('/collections', '/collection-metadata');
                const response = await fetch(collectionMetadataUrl);
            if (!response.ok) {
                throw new Error('Failed to fetch collections');
            }

            const collections = await response.json();
            console.log('Fetched collections:', collections);

            // Clear existing options (except the default)
            while (collectionDropdown.options.length > 1) {
                collectionDropdown.remove(1);
            }

            // Add options for each collection
            collections.forEach(collection => {
                const option = document.createElement('option');
                option.value = collection.name;
                option.textContent = `${collection.name} (${collection.firestoreCollection})`;
                console.log('Adding collection option:', collection.name, 'with storagePrefix:', collection.storagePrefix);

                // Select this option if it matches the selectedCollectionId
                if (selectedCollectionId && collection.name === selectedCollectionId) {
                    option.selected = true;
                }

                collectionDropdown.appendChild(option);
            });
        } catch (error) {
            console.error('Error fetching collections:', error);
            messageDiv.textContent = 'Failed to load collections. You can still upload cards without selecting a collection.';
            messageDiv.className = 'warning';
        }
    }
}); 
