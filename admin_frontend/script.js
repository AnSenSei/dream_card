document.addEventListener('DOMContentLoaded', () => {
    const uploadForm = document.getElementById('uploadForm');
    const messageDiv = document.getElementById('message');
    const collectionDropdown = document.getElementById('collection_metadata_id');

    // Use the full backend URL including the scheme, host, port, and full path
    const backendUrl = 'http://localhost:8080/gacha/api/v1/storage/upload_card';
    const collectionsUrl = 'http://localhost:8080/gacha/api/v1/storage/collection-metadata';

    // Check URL parameters for collection_metadata_id
    const urlParams = new URLSearchParams(window.location.search);
    const collectionMetadataIdFromUrl = urlParams.get('collection_metadata_id');

    // Fetch collection metadata and populate dropdown
    fetchCollections(collectionMetadataIdFromUrl);

    uploadForm.addEventListener('submit', async (event) => {
        event.preventDefault();
        messageDiv.textContent = '';
        messageDiv.className = '';

        const formData = new FormData();
        formData.append('card_name', document.getElementById('card_name').value);
        formData.append('rarity', document.getElementById('rarity').value);
        formData.append('point_worth', document.getElementById('point_worth').value);
        formData.append('quantity', document.getElementById('quantity').value);
        formData.append('date_got_in_stock', document.getElementById('date_got_in_stock').value);

        // Add collection metadata ID if provided
        const collectionMetadataId = document.getElementById('collection_metadata_id').value;
        console.log('Collection dropdown value:', collectionMetadataId);

        // Prepare the URL with the collection_metadata_id as a query parameter if provided
        let uploadUrl = backendUrl;
        if (collectionMetadataId) {
            uploadUrl = `${backendUrl}?collection_metadata_id=${encodeURIComponent(collectionMetadataId)}`;
            console.log('Sending collection_metadata_id as query parameter:', collectionMetadataId);
            console.log('Upload URL with collection_metadata_id:', uploadUrl);
        }

        const imageFile = document.getElementById('image_file').files[0];
        if (!imageFile) {
            messageDiv.textContent = 'Please select an image file.';
            messageDiv.className = 'error';
            return;
        }
        formData.append('image_file', imageFile);

        try {
            const response = await fetch(uploadUrl, {
                method: 'POST',
                body: formData, // FormData will set the Content-Type to multipart/form-data automatically
            });

            const result = await response.json();

            if (response.ok) {
                messageDiv.textContent = 'Card uploaded successfully! Card Name: ' + result.card_name + ', Image URL: ' + result.image_url;
                messageDiv.className = 'success';
                uploadForm.reset();
            } else {
                let errorMessage = 'Error uploading card.';
                if (result.detail) {
                    if (typeof result.detail === 'string') {
                        errorMessage += ' Server says: ' + result.detail;
                    } else if (Array.isArray(result.detail)) { // Handle FastAPI validation errors
                        errorMessage += ' Details: ' + result.detail.map(err => `Field: ${err.loc[1]}, Message: ${err.msg}`).join('; ');
                    } else if (typeof result.detail === 'object'){
                        errorMessage += ' Server says: ' + JSON.stringify(result.detail);
                    }
                }
                messageDiv.textContent = errorMessage;
                messageDiv.className = 'error';
            }
        } catch (error) {
            console.error('Upload error:', error);
            messageDiv.textContent = 'An unexpected error occurred. Check the console for details.';
            messageDiv.className = 'error';
        }
    });

    // Function to fetch collection metadata and populate dropdown
    async function fetchCollections(selectedCollectionId = null) {
        try {
            const response = await fetch(collectionsUrl);
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
