document.addEventListener('DOMContentLoaded', async () => {
    const cardDisplayArea = document.getElementById('cardDisplayArea');
    const loadingMessage = document.getElementById('loadingMessage');
    const errorMessageDiv = document.getElementById('errorMessage');
    
    // Inputs and Controls
    const collectionNameInput = document.getElementById('collectionNameInput');
    const fetchCollectionButton = document.getElementById('fetchCollectionButton');
    const sortBySelect = document.getElementById('sortBySelect');
    const sortOrderSelect = document.getElementById('sortOrderSelect');
    const perPageSelect = document.getElementById('perPageSelect');
    const paginationControlsTop = document.getElementById('paginationControlsTop');
    const paginationControlsBottom = document.getElementById('paginationControlsBottom');
    const searchInput = document.getElementById('searchInput');
    const searchButton = document.getElementById('searchButton');
    const clearSearchButton = document.getElementById('clearSearchButton');

    const baseUrl = 'http://localhost:8080/gacha/api/v1/storage';

    // State variables
    let currentPage = 1;
    let itemsPerPage = parseInt(perPageSelect.value, 10);
    let currentSortBy = sortBySelect.value;
    let currentSortOrder = sortOrderSelect.value;
    let currentCollectionName = collectionNameInput.value.trim() || null;
    let currentSearchQuery = searchInput.value.trim() || null;
    let totalPages = 1;

    function displayError(message) {
        errorMessageDiv.textContent = message;
        errorMessageDiv.style.display = 'block';
        loadingMessage.style.display = 'none';
        cardDisplayArea.innerHTML = ''; // Clear cards area on error
        renderPaginationControls(); // Clear pagination controls
    }

    async function fetchAndDisplayCards() {
        loadingMessage.style.display = 'block';
        errorMessageDiv.style.display = 'none';
        cardDisplayArea.innerHTML = ''; // Clear previous cards

            // Get collection name from URL if not already set
            if (!currentCollectionName) {
                const urlParams = new URLSearchParams(window.location.search);
                const collectionParam = urlParams.get('collectionName');
                if (collectionParam) {
                    currentCollectionName = collectionParam;
                    console.log(`Set currentCollectionName from URL: ${currentCollectionName}`);
                }
            }
    
                // Parse URL params before each fetch if not already set
                if (!currentCollectionName) {
                    parseUrlParams();
                }
        
                // Build URL with proper query parameters
                const queryParams = new URLSearchParams();
                queryParams.append('page', currentPage);
                queryParams.append('per_page', itemsPerPage);
                queryParams.append('sort_by', currentSortBy);
                queryParams.append('sort_order', currentSortOrder);
                
                if (currentCollectionName && currentCollectionName.trim() !== '') {
                    queryParams.append('collectionName', currentCollectionName.trim());
                    console.log(`Adding collection name to fetch: ${currentCollectionName}`);
                }
                
                if (currentSearchQuery && currentSearchQuery.trim() !== '') {
                    queryParams.append('search_query', currentSearchQuery.trim());
                    clearSearchButton.style.display = 'inline-block';
                } else {
                    clearSearchButton.style.display = 'none';
                }
                
                const url = `${baseUrl}/cards?${queryParams.toString()}`;
                console.log(`Fetching cards from: ${url}`);
        if (currentSearchQuery && currentSearchQuery.trim() !== '') {
            url += `&search_query=${encodeURIComponent(currentSearchQuery.trim())}`;
            clearSearchButton.style.display = 'inline-block';
        } else {
            clearSearchButton.style.display = 'none';
        }
        
        console.log(`Fetching cards from: ${url}`);

        try {
            const response = await fetch(url);

            if (!response.ok) {
                let errorMsg = `Error fetching cards. Status: ${response.status}`;
                try {
                    const errorResult = await response.json();
                    if (errorResult.detail) {
                        errorMsg += ', Server says: ' + (typeof errorResult.detail === 'string' ? errorResult.detail : JSON.stringify(errorResult.detail));
                    }
                } catch (e) {
                    const textError = await response.text();
                    if (textError) errorMsg += ', Response: ' + textError;
                }
                throw new Error(errorMsg);
            }

            const result = await response.json();
            console.log('API Response Result:', result); // Log the whole result object
            loadingMessage.style.display = 'none';

            // Robust check for result and result.cards
            if (!result || typeof result !== 'object') {
                console.error('API response is not an object:', result);
                displayError('Invalid data structure received from server.');
                renderPaginationControls({ total_pages: 0, current_page: currentPage, total_items: 0, per_page: itemsPerPage }); // Clear/reset pagination
                updateActiveControls();
                return; 
            }

            if (!result.cards || !Array.isArray(result.cards)) {
                console.error('result.cards is missing or not an array. Actual result.cards:', result.cards);
                cardDisplayArea.innerHTML = '<p>No cards data found in the expected format (array).</p>';
                totalPages = 0;
            } else if (result.cards.length === 0) {
                cardDisplayArea.innerHTML = '<p>No cards found matching your criteria.</p>';
                totalPages = 0;
            } else {
                result.cards.forEach(card => {
                    const cardElement = document.createElement('div');
                    cardElement.classList.add('card-item');
                    cardElement.dataset.cardId = card.id;

                    let imageHtml = '<div class="card-image-placeholder">No Image</div>';
                    if (card.image_url) {
                        imageHtml = `
                            <img 
                                src="${card.image_url}" 
                                alt="${card.card_name}" 
                                style="width:100px; height:auto;"
                                onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';"
                            >
                            <div class="card-image-placeholder" style="display:none;">
                                Image Failed to Load
                            </div>
                        `;
                    }

                    cardElement.innerHTML = `
                        <div class="card-image">${imageHtml}</div>
                        <div class="card-details">
                            <h3>${card.card_name}</h3>
                            <p><strong>Rarity:</strong> ${card.rarity}</p>
                            <p><strong>Point Worth:</strong> ${card.point_worth}</p>
                            <p><strong>Date In Stock:</strong> ${card.date_got_in_stock}</p>
                            <p><strong>Quantity:</strong> <span class="quantity-value">${card.quantity || 0}</span></p>
                            <div class="quantity-controls">
                                <button onclick="updateQuantity('${card.id}', 1, '${currentCollectionName || ''}')" class="quantity-btn">+1</button>
                                <button onclick="updateQuantity('${card.id}', -1, '${currentCollectionName || ''}')" class="quantity-btn">-1</button>
                                <input type="number" class="quantity-input" placeholder="Set Qty">
                                <button onclick="updateQuantityByInput('${card.id}', this.previousElementSibling.value, '${currentCollectionName || ''}')" class="quantity-btn">Set</button>
                            </div>
                        </div>
                    `;
                    cardDisplayArea.appendChild(cardElement);
                });
                // Safely access pagination details
                totalPages = result.pagination && result.pagination.total_pages ? result.pagination.total_pages : 0;
            }
            
            // Safely render pagination controls
            if (result.pagination && typeof result.pagination === 'object') {
                renderPaginationControls(result.pagination);
            } else {
                console.warn("Pagination data missing or malformed in API response. Received:", result.pagination);
                renderPaginationControls({ 
                    total_items: (result.cards && Array.isArray(result.cards) ? result.cards.length : 0),
                    total_pages: totalPages, 
                    current_page: currentPage, 
                    per_page: itemsPerPage 
                });
            }
            updateActiveControls(); // Reflect current sort/filter state in UI selectors

        } catch (error) {
            console.error('Failed to load cards:', error);
            displayError('Failed to load cards. ' + error.message);
        }
    }

    function renderPaginationControls(paginationData) {
        paginationControlsTop.innerHTML = '';
        paginationControlsBottom.innerHTML = '';

        if (!paginationData || paginationData.total_pages <= 0) {
            return; // No pagination needed if no pages or no items
        }

        const { total_pages, current_page } = paginationData;

        const createButton = (text, page, isDisabled = false, isCurrent = false) => {
            const button = document.createElement('button');
            button.textContent = text;
            button.disabled = isDisabled;
            if (isCurrent) {
                button.classList.add('current-page');
            }
            button.addEventListener('click', () => {
                currentPage = page;
                fetchAndDisplayCards();
            });
            return button;
        };

        const pageInfo = document.createElement('span');
        pageInfo.textContent = `Page ${current_page} of ${total_pages}`; 
        pageInfo.style.margin = "0 10px";

        const prevButton = createButton('Previous', current_page - 1, current_page === 1);
        const nextButton = createButton('Next', current_page + 1, current_page === total_pages);

        // Clone for bottom controls
        const paginationFragment = document.createDocumentFragment();
        paginationFragment.appendChild(prevButton.cloneNode(true));
        paginationFragment.appendChild(pageInfo.cloneNode(true));
        paginationFragment.appendChild(nextButton.cloneNode(true));
        
        paginationControlsTop.appendChild(prevButton);
        paginationControlsTop.appendChild(pageInfo);
        paginationControlsTop.appendChild(nextButton);

        // Re-attach event listeners for cloned nodes if needed, or better, create new nodes for bottom
        const prevButtonBottom = createButton('Previous', current_page - 1, current_page === 1);
        const nextButtonBottom = createButton('Next', current_page + 1, current_page === total_pages);
        const pageInfoBottom = document.createElement('span');
        pageInfoBottom.textContent = `Page ${current_page} of ${total_pages}`; 
        pageInfoBottom.style.margin = "0 10px";

        paginationControlsBottom.appendChild(prevButtonBottom);
        paginationControlsBottom.appendChild(pageInfoBottom);
        paginationControlsBottom.appendChild(nextButtonBottom);
    }

    function updateActiveControls() {
        sortBySelect.value = currentSortBy;
        sortOrderSelect.value = currentSortOrder;
        perPageSelect.value = itemsPerPage.toString();
        collectionNameInput.value = currentCollectionName || '';
        searchInput.value = currentSearchQuery || '';
    }

    // Event Listeners for controls
    fetchCollectionButton.addEventListener('click', () => {
        currentCollectionName = collectionNameInput.value.trim() || null;
        currentPage = 1; // Reset to first page
        fetchAndDisplayCards();
    });

    searchButton.addEventListener('click', () => {
        currentSearchQuery = searchInput.value.trim() || null;
        currentPage = 1; // Reset to first page on new search
        fetchAndDisplayCards();
    });

    searchInput.addEventListener('keypress', (event) => {
        if (event.key === 'Enter') {
            searchButton.click(); // Trigger search on Enter key
        }
    });

    clearSearchButton.addEventListener('click', () => {
        currentSearchQuery = null;
        searchInput.value = ''; // Clear the input field
        currentPage = 1;
        clearSearchButton.style.display = 'none';
        fetchAndDisplayCards();
    });

    sortBySelect.addEventListener('change', (e) => {
        currentSortBy = e.target.value;
        currentPage = 1;
        fetchAndDisplayCards();
    });

    sortOrderSelect.addEventListener('change', (e) => {
        currentSortOrder = e.target.value;
        currentPage = 1;
        fetchAndDisplayCards();
    });

    perPageSelect.addEventListener('change', (e) => {
        itemsPerPage = parseInt(e.target.value, 10);
        currentPage = 1;
        fetchAndDisplayCards();
    });

    // Initial load
    fetchAndDisplayCards(); 
});

// Keep existing quantity update functions, but ensure they know about collectionName for future backend use if necessary
// Modify updateQuantity and updateQuantityByInput to accept collectionName if your backend requires it for PATCH.
// For now, the PATCH endpoint in storage_router.py does take collectionName as an optional query param.

window.updateQuantity = async function(documentId, change, collectionName = null) {
    let url = `http://localhost:8080/gacha/api/v1/storage/cards/${documentId}/quantity`;
    if (collectionName && collectionName.trim() !== '') {
        url += `?collectionName=${encodeURIComponent(collectionName.trim())}`;
    }

    try {
        const response = await fetch(url, {
            method: 'PATCH',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ quantity_change: change })
        });

        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || `Error: ${response.status}`);
        }

        const updatedCard = await response.json();
        const cardElement = document.querySelector(`[data-card-id="${documentId}"]`);
        if (cardElement) {
            cardElement.querySelector('.quantity-value').textContent = updatedCard.quantity;
        }
    } catch (error) {
        console.error('Failed to update quantity:', error);
        alert(`Failed to update quantity: ${error.message}`);
    }
}

window.updateQuantityByInput = async function(documentId, inputValue, collectionName = null) {
    // If inputValue is from a direct quantity update (not a change)
    // The current backend /quantity endpoint expects a 'quantity_change'.
    // If you want to set absolute quantity, you'd need a different endpoint or logic.
    // For now, assuming this button means to 'add' or 'subtract' this amount if interpreted as a change.
    // OR, if it's meant to 'set' the quantity, we need to calculate the 'change'.
    // The input placeholder is "Set Qty", let's adjust this to set the quantity directly.
    // This would require either:
    // 1. Fetching current quantity, calculating change, then calling existing endpoint.
    // 2. Modifying backend to accept absolute quantity on the PATCH /quantity (less RESTful for this specific endpoint).
    // 3. Using the PUT /cards/{document_id} endpoint if it can update just quantity.

    // For simplicity with current backend PATCH /quantity endpoint, let's interpret as a change for now
    // or demonstrate how to use PUT if user wants to set absolute.
    // The button text is "Set", let's assume it's to set absolute quantity.
    // We need to use the PUT endpoint for this or modify PATCH.
    // Given current backend, let's change this button to increment/decrement by the input amount for now.
    
    const amount = parseInt(inputValue);
    if (isNaN(amount)) {
        alert('Please enter a valid number for quantity change.');
        return;
    }

    // This will call the existing updateQuantity which sends a 'quantity_change'
    await window.updateQuantity(documentId, amount, collectionName); 

    const cardElement = document.querySelector(`[data-card-id="${documentId}"]`);
    if (cardElement) {
        cardElement.querySelector('.quantity-input').value = ''; // Clear input after update
    }
    // No, the user wants to set the quantity, so we should use the update_card_endpoint instead.
    // We'll make a new function for this or adjust.
    // For now, let's change the button behavior to actually *set* quantity via the PUT endpoint.
    // This is more complex as it requires knowing all other fields if the PUT expects a full card object.
    // The PUT endpoint takes `UpdateCardRequest` which allows partial updates.

    // Let's make this button set the absolute quantity using the PUT /cards/{document_id} endpoint.
    const newQuantity = parseInt(inputValue);
    if (isNaN(newQuantity) || newQuantity < 0) {
        alert('Please enter a valid non-negative number for quantity.');
        return;
    }

    let updateUrl = `http://localhost:8080/gacha/api/v1/storage/cards/${documentId}`;
    if (collectionName && collectionName.trim() !== '') {
        updateUrl += `?collectionName=${encodeURIComponent(collectionName.trim())}`;
    }

    try {
        const response = await fetch(updateUrl, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ quantity: newQuantity }) // Send only the quantity to update
        });

        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || `Error: ${response.status}`);
        }
        const updatedCard = await response.json();
        const cardElement = document.querySelector(`[data-card-id="${documentId}"]`);
        if (cardElement) {
            cardElement.querySelector('.quantity-value').textContent = updatedCard.quantity;
            cardElement.querySelector('.quantity-input').value = ''; // Clear input
        }

    } catch (error) {
        console.error('Failed to set quantity:', error);
        alert(`Failed to set quantity: ${error.message}`);
    }
} 