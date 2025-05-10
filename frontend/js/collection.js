alert("Attempting to load collection.js");
console.log("collection.js loaded");

// In a real application, card collection data would be fetched from a backend
// or managed using localStorage for a simple frontend-only toy.

// Initialize the card editing feature when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    enableCardNameEditing();
});

// API endpoints
const API_BASE_URL = 'http://localhost:8080/gacha/api/v1/storage';
const ENDPOINTS = {
    GET_CARDS: (collectionName = null) => {
        let url = `${API_BASE_URL}/cards`;
        if (collectionName && collectionName.trim() !== '') {
            url += `?collectionName=${encodeURIComponent(collectionName.trim())}`;
        }
        return url;
    },
    UPDATE_CARD: (id, collectionName = null) => {
        let url = `${API_BASE_URL}/cards/${id}`;
        if (collectionName && collectionName.trim() !== '') {
            url += `?collectionName=${encodeURIComponent(collectionName.trim())}`;
        }
        return url;
    }
};

// Wait for DOM to be fully loaded
document.addEventListener('DOMContentLoaded', () => {
    console.log("DOM fully loaded");
    
    // DOM Elements - move inside DOMContentLoaded to ensure elements exist
    const modal = document.getElementById('edit-modal');
    const closeBtn = document.querySelector('.close');
    const editForm = document.getElementById('edit-card-form');
    const cancelBtn = document.getElementById('cancel-edit');
    
    if (!modal || !closeBtn || !editForm || !cancelBtn) {
        console.error('Required DOM elements not found:', {
            modal: !!modal,
            closeBtn: !!closeBtn,
            editForm: !!editForm,
            cancelBtn: !!cancelBtn
        });
        return;
    }

    // State management
    let currentCards = [];
    let currentCollectionName = null;

    // DOM Elements for collection switching
    const tabButtons = document.querySelectorAll('.tab-button');
    const customCollectionNameInput = document.getElementById('customCollectionName');
    const viewCustomCollectionButton = document.getElementById('viewCustomCollection');

    async function fetchCards(collectionName = null) {
        currentCollectionName = collectionName;
        // Update active tab
        tabButtons.forEach(button => {
            if (button.dataset.collection === (collectionName || '')) { // Match empty string for default
                button.classList.add('active');
            } else {
                button.classList.remove('active');
            }
        });
        // If a custom collection is active that doesn't match a tab, clear tab active states
        if (!Array.from(tabButtons).some(btn => btn.dataset.collection === (collectionName || ''))) {
            tabButtons.forEach(button => button.classList.remove('active'));
        }

        try {
            const url = ENDPOINTS.GET_CARDS(collectionName);
            console.log(`Fetching cards from: ${url}`);
            const response = await fetch(url);
            if (!response.ok) throw new Error('Failed to fetch cards');
            const cards = await response.json();
            currentCards = cards;
            displayCollection(cards);
        } catch (error) {
            console.error('Error fetching cards:', error);
            displayError('Failed to load cards. Please try again later.');
        }
    }

    function displayCollection(cards) {
        const collectionArea = document.getElementById('my-cards-area');
        const totalCardsSpan = document.getElementById('total-cards');
        const uniqueCardsSpan = document.getElementById('unique-cards');

        if (!cards || cards.length === 0) {
            collectionArea.innerHTML = '<p>Your collection is empty. Go draw some cards!</p>';
            totalCardsSpan.textContent = '0';
            uniqueCardsSpan.textContent = '0';
            return;
        }

        collectionArea.innerHTML = ''; // Clear area
        let uniqueIds = new Set();

        cards.forEach(card => {
            uniqueIds.add(card.id);
            const cardElement = document.createElement('div');
            cardElement.classList.add('card-item');
            cardElement.classList.add(`rarity-${card.rarity.toLowerCase()}`);
            cardElement.innerHTML = `
                <img src="${card.image_url || 'https://via.placeholder.com/150/CCCCCC/000000?Text=No+Image'}" alt="${card.card_name}">
                <h4>${card.card_name}</h4>
                <p>Rarity: ${card.rarity}</p>
                <p>Quantity: ${card.quantity}</p>
                <p>Points: ${card.point_worth}</p>
            `;
            
            collectionArea.appendChild(cardElement);

            // Find the h4 element within the cardElement
            const cardNameElement = cardElement.querySelector('h4');
            if (cardNameElement) {
                // Add click handler for editing to the card name
                cardNameElement.addEventListener('click', (event) => {
                    event.stopPropagation(); // Prevent click from bubbling to parent
                    console.log('Card name clicked:', card);
                    openEditModal(card);
                });
            } else {
                console.warn('Card name element (h4) not found for card:', card);
            }
        });

        totalCardsSpan.textContent = cards.reduce((sum, card) => sum + card.quantity, 0);
        uniqueCardsSpan.textContent = uniqueIds.size;
    }

    function openEditModal(card) {
        console.log('Opening modal for card:', card);
        // Populate form fields
        document.getElementById('card-id').value = card.id;
        document.getElementById('edit-collection-name').value = currentCollectionName || '';
        document.getElementById('card-name').value = card.card_name;
        document.getElementById('card-rarity').value = card.rarity;
        document.getElementById('card-points').value = card.point_worth;
        document.getElementById('card-quantity').value = card.quantity;
        document.getElementById('card-date').value = card.date_got_in_stock;
        
        // Show modal
        modal.classList.add('show');
        console.log('Modal should be visible now with class "show"');
    }

    // Enable card name click-to-edit feature
    function enableCardNameEditing() {
        document.addEventListener('click', function(event) {
            // Check if the clicked element is a card name
            if (event.target.classList.contains('card-name')) {
                const cardId = event.target.closest('.card-item').dataset.cardId;
                openEditModal(cardId);
            }
        });
    }
    
    // Open edit modal and populate with card data
    function openEditModal(cardId) {
        const card = currentCards.find(card => card.id === cardId);
        if (!card) return;
        
        // Populate the edit form with card data
        const form = document.getElementById('edit-card-form');
        form.querySelector('input[name="card-id"]').value = card.id;
        form.querySelector('input[name="card-name"]').value = card.card_name;
        form.querySelector('input[name="card-rarity"]').value = card.rarity;
        form.querySelector('input[name="card-points"]').value = card.point_worth;
        form.querySelector('input[name="card-quantity"]').value = card.quantity;
        form.querySelector('input[name="card-date"]').value = card.date_got_in_stock;
        
        // Set the collection name if available
        const collectionNameInput = form.querySelector('input[name="edit-collection-name"]');
        if (collectionNameInput && currentCollection) {
            collectionNameInput.value = currentCollection;
        }
        
        // Show the modal
        const modal = document.getElementById('edit-card-modal');
        modal.style.display = 'block';
    }
    
    // Close the edit modal
    function closeModal() {
        const modal = document.getElementById('edit-card-modal');
        if (modal) modal.style.display = 'none';
    }
    
    // Display success message
    function displaySuccess(message) {
        // Check if notification container exists, create if not
        let notificationContainer = document.getElementById('notification-container');
        if (!notificationContainer) {
            notificationContainer = document.createElement('div');
            notificationContainer.id = 'notification-container';
            notificationContainer.style.position = 'fixed';
            notificationContainer.style.top = '20px';
            notificationContainer.style.right = '20px';
            notificationContainer.style.zIndex = '1000';
            document.body.appendChild(notificationContainer);
        }
        
        const notification = document.createElement('div');
        notification.className = 'notification success';
        notification.innerHTML = message;
        notification.style.backgroundColor = '#4CAF50';
        notification.style.color = 'white';
        notification.style.padding = '10px';
        notification.style.marginBottom = '10px';
        notification.style.borderRadius = '4px';
        notification.style.boxShadow = '0 2px 4px rgba(0,0,0,0.2)';
        
        notificationContainer.appendChild(notification);
        
        // Remove notification after 3 seconds
        setTimeout(() => {
            notification.remove();
        }, 3000);
    }
    
    // Display error message
    function displayError(message) {
        // Check if notification container exists, create if not
        let notificationContainer = document.getElementById('notification-container');
        if (!notificationContainer) {
            notificationContainer = document.createElement('div');
            notificationContainer.id = 'notification-container';
            notificationContainer.style.position = 'fixed';
            notificationContainer.style.top = '20px';
            notificationContainer.style.right = '20px';
            notificationContainer.style.zIndex = '1000';
            document.body.appendChild(notificationContainer);
        }
        
        const notification = document.createElement('div');
        notification.className = 'notification error';
        notification.innerHTML = message;
        notification.style.backgroundColor = '#f44336';
        notification.style.color = 'white';
        notification.style.padding = '10px';
        notification.style.marginBottom = '10px';
        notification.style.borderRadius = '4px';
        notification.style.boxShadow = '0 2px 4px rgba(0,0,0,0.2)';
        
        notificationContainer.appendChild(notification);
        
        // Remove notification after 3 seconds
        setTimeout(() => {
            notification.remove();
        }, 3000);
    }
    
    async function updateCard(formData) {
        const cardId = formData.get('card-id');
        const collectionName = formData.get('edit-collection-name');
        try {
            const response = await fetch(ENDPOINTS.UPDATE_CARD(cardId, collectionName), {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    card_name: formData.get('card-name'),
                    rarity: formData.get('card-rarity'),
                    point_worth: parseInt(formData.get('card-points')),
                    quantity: parseInt(formData.get('card-quantity')),
                    date_got_in_stock: formData.get('card-date')
                })
            });

            if (!response.ok) throw new Error('Failed to update card');
            
            const updatedCard = await response.json();
            // Update the card in the current cards array
            const index = currentCards.findIndex(card => card.id === cardId);
            if (index !== -1) {
                currentCards[index] = updatedCard;
                displayCollection(currentCards);
            }
            
            closeModal();
            displaySuccess('Card updated successfully!');
        } catch (error) {
            console.error('Error updating card:', error);
            displayError('Failed to update card. Please try again.');
        }
    }

    function closeModal() {
        console.log('Closing modal');
        modal.classList.remove('show');
        editForm.reset();
    }

    function displayError(message) {
        console.error('Error:', message);
        alert(message);
    }

    function displaySuccess(message) {
        console.log('Success:', message);
        alert(message);
    }

    // Event Listeners
    editForm.addEventListener('submit', (e) => {
        e.preventDefault();
        console.log('Form submitted');
        const formData = new FormData(editForm);
        updateCard(formData);
    });

    closeBtn.addEventListener('click', () => {
        console.log('Close button clicked');
        closeModal();
    });

    cancelBtn.addEventListener('click', () => {
        console.log('Cancel button clicked');
        closeModal();
    });

    // Close modal when clicking outside
    window.addEventListener('click', (e) => {
        if (e.target === modal) {
            console.log('Clicked outside modal');
            closeModal();
        }
    });

    // Event listeners for collection tabs
    tabButtons.forEach(button => {
        button.addEventListener('click', () => {
            const collectionName = button.dataset.collection;
            customCollectionNameInput.value = ''; // Clear custom input
            fetchCards(collectionName === '' ? null : collectionName); // Pass null for default
        });
    });

    // Event listener for custom collection button
    if (viewCustomCollectionButton) {
        viewCustomCollectionButton.addEventListener('click', () => {
            const collectionName = customCollectionNameInput.value.trim();
            if (collectionName) {
                fetchCards(collectionName);
            } else {
                // Optionally, fetch default or show an error if input is empty
                fetchCards(null); 
            }
        });
    }

    // Initial fetch of cards - e.g., fetch "my_cards" or default
    // Let's make the first tab active by default
    const defaultTab = document.querySelector('.tab-button[data-collection="my_cards"]');
    if (defaultTab) {
        fetchCards("my_cards");
    } else {
        fetchCards(); // Fallback to backend default if "my_cards" tab doesn't exist
    }
});

// To make this interactive with draws, you'd need to:
// 1. Store drawn cards (e.g., in localStorage or send to a backend and fetch here).
// 2. Update mockCollection (or fetch the real collection) and re-call displayCollection(). 