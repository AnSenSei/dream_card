// Global variables
const ENDPOINTS = {
    GET_ALL_PACKS: '/api/packs',
    GET_PACK: (packId) => `/api/packs/${packId}`,
    UPDATE_PACK: (packId) => `/api/packs/${packId}/update`
};

let currentPack = null;

// DOM Content Loaded
document.addEventListener('DOMContentLoaded', function() {
    // Initial setup
    fetchAllPacks();
    
    // Set up event listeners
    document.getElementById('update-pack-form')?.addEventListener('submit', handlePackUpdate);
});

// Fetch all packs
async function fetchAllPacks() {
    try {
        const response = await fetch(ENDPOINTS.GET_ALL_PACKS);
        if (!response.ok) {
            throw new Error('Failed to fetch packs');
        }
        
        const packs = await response.json();
        displayPacks(packs);
    } catch (error) {
        console.error('Error fetching packs:', error);
        displayError('Failed to load packs. Please try again.');
    }
}

// Display packs in the packs table
function displayPacks(packs) {
    const packsTableBody = document.getElementById('packs-table-body');
    if (!packsTableBody) return;
    
    packsTableBody.innerHTML = '';
    
    packs.forEach(pack => {
        const row = document.createElement('tr');
        
        // Pack ID
        const idCell = document.createElement('td');
        idCell.textContent = pack.id;
        row.appendChild(idCell);
        
        // Pack Name (clickable)
        const nameCell = document.createElement('td');
        const nameLink = document.createElement('a');
        nameLink.href = '#';
        nameLink.textContent = pack.name;
        nameLink.classList.add('pack-name-link');
        nameLink.dataset.packId = pack.id;
        nameLink.addEventListener('click', (e) => {
            e.preventDefault();
            fetchPackDetails(pack.id);
        });
        nameCell.appendChild(nameLink);
        row.appendChild(nameCell);
        
        // Pack Image
        const imageCell = document.createElement('td');
        if (pack.image_url) {
            const img = document.createElement('img');
            img.src = pack.image_url;
            img.alt = pack.name;
            img.style.width = '50px';
            img.style.height = 'auto';
            imageCell.appendChild(img);
        } else {
            imageCell.textContent = 'No image';
        }
        row.appendChild(imageCell);
        
        // Actions column
        const actionsCell = document.createElement('td');
        const editBtn = document.createElement('button');
        editBtn.textContent = 'Edit';
        editBtn.classList.add('btn', 'btn-sm', 'btn-primary', 'mr-2');
        editBtn.addEventListener('click', () => fetchPackDetails(pack.id));
        actionsCell.appendChild(editBtn);
        row.appendChild(actionsCell);
        
        packsTableBody.appendChild(row);
    });
}

// Fetch details for a specific pack
async function fetchPackDetails(packId) {
    try {
        const response = await fetch(ENDPOINTS.GET_PACK(packId));
        if (!response.ok) {
            throw new Error('Failed to fetch pack details');
        }
        
        const pack = await response.json();
        currentPack = pack;
        showPackDetailsModal(pack);
    } catch (error) {
        console.error('Error fetching pack details:', error);
        displayError('Failed to load pack details. Please try again.');
    }
}

// Display pack details in modal
function showPackDetailsModal(pack) {
    // Set basic pack info
    document.getElementById('pack-id-display').textContent = pack.id;
    document.getElementById('pack-name-input').value = pack.name;
    document.getElementById('pack-description-input').value = pack.description || '';
    
    // Clear existing rarity fields
    const raritiesContainer = document.getElementById('rarities-container');
    raritiesContainer.innerHTML = '';
    
    // Add fields for each rarity
    if (pack.rarity_configurations) {
        Object.entries(pack.rarity_configurations).forEach(([rarityName, rarityData]) => {
            const rarityDiv = document.createElement('div');
            rarityDiv.classList.add('rarity-section', 'border', 'p-3', 'mb-3');
            
            // Rarity header
            const rarityHeader = document.createElement('h5');
            rarityHeader.textContent = `Rarity: ${rarityName}`;
            rarityDiv.appendChild(rarityHeader);
            
            // Probability field
            const probabilityGroup = document.createElement('div');
            probabilityGroup.classList.add('form-group');
            
            const probabilityLabel = document.createElement('label');
            probabilityLabel.textContent = 'Probability:';
            probabilityGroup.appendChild(probabilityLabel);
            
            const probabilityInput = document.createElement('input');
            probabilityInput.type = 'number';
            probabilityInput.step = '0.01';
            probabilityInput.min = '0';
            probabilityInput.max = '1';
            probabilityInput.classList.add('form-control');
            probabilityInput.name = `rarities.${rarityName}.probability`;
            probabilityInput.value = rarityData.probability || 0;
            probabilityGroup.appendChild(probabilityInput);
            
            rarityDiv.appendChild(probabilityGroup);
            
            // Cards section
            if (rarityData.cards && rarityData.cards.length > 0) {
                const cardsHeader = document.createElement('h6');
                cardsHeader.textContent = 'Cards:';
                rarityDiv.appendChild(cardsHeader);
                
                const cardsList = document.createElement('ul');
                cardsList.classList.add('cards-list');
                
                rarityData.cards.forEach(cardId => {
                    const cardItem = document.createElement('li');
                    cardItem.classList.add('d-flex', 'justify-content-between', 'align-items-center', 'mb-1');
                    
                    const cardText = document.createElement('span');
                    cardText.textContent = cardId;
                    cardItem.appendChild(cardText);
                    
                    const deleteBtn = document.createElement('button');
                    deleteBtn.type = 'button';
                    deleteBtn.classList.add('btn', 'btn-sm', 'btn-danger');
                    deleteBtn.textContent = 'Remove';
                    deleteBtn.dataset.rarity = rarityName;
                    deleteBtn.dataset.cardId = cardId;
                    deleteBtn.addEventListener('click', function() {
                        addCardToDeleteList(rarityName, cardId);
                        cardItem.remove();
                    });
                    cardItem.appendChild(deleteBtn);
                    
                    cardsList.appendChild(cardItem);
                });
                
                rarityDiv.appendChild(cardsList);
            } else {
                const noCardsMsg = document.createElement('p');
                noCardsMsg.textContent = 'No cards in this rarity.';
                rarityDiv.appendChild(noCardsMsg);
            }
            
            // Add cards section
            const addCardsGroup = document.createElement('div');
            addCardsGroup.classList.add('form-group', 'mt-3');
            
            const addCardsLabel = document.createElement('label');
            addCardsLabel.textContent = 'Add Cards (comma-separated IDs):';
            addCardsGroup.appendChild(addCardsLabel);
            
            const addCardsInput = document.createElement('input');
            addCardsInput.type = 'text';
            addCardsInput.classList.add('form-control');
            addCardsInput.name = `cards_to_add.${rarityName}`;
            addCardsInput.placeholder = 'XY-001, XY-002, ...';
            addCardsGroup.appendChild(addCardsInput);
            
            rarityDiv.appendChild(addCardsGroup);
            raritiesContainer.appendChild(rarityDiv);
        });
    }
    
    // Initialize the hidden fields for cards to delete
    document.getElementById('cards-to-delete').value = JSON.stringify({});
    
    // Show the modal
    const packModal = new bootstrap.Modal(document.getElementById('pack-details-modal'));
    packModal.show();
}

// Add a card to the delete list (stored in a hidden field)
function addCardToDeleteList(rarity, cardId) {
    const deleteListField = document.getElementById('cards-to-delete');
    let deleteList = JSON.parse(deleteListField.value || '{}');
    
    if (!deleteList[rarity]) {
        deleteList[rarity] = [];
    }
    
    deleteList[rarity].push(cardId);
    deleteListField.value = JSON.stringify(deleteList);
}

// Handle pack update form submission
async function handlePackUpdate(event) {
    event.preventDefault();
    
    const packId = document.getElementById('pack-id-display').textContent;
    const form = event.target;
    const formData = new FormData(form);
    
    // Prepare the update object
    const updates = {
        rarities: {},
        cards_to_add: {},
        cards_to_delete: JSON.parse(document.getElementById('cards-to-delete').value || '{}')
    };
    
    // Process form data
    for (const [key, value] of formData.entries()) {
        if (key.startsWith('rarities.')) {
            // Handle rarity property updates (like probability)
            const [_, rarityName, property] = key.split('.');
            if (!updates.rarities[rarityName]) {
                updates.rarities[rarityName] = {};
            }
            updates.rarities[rarityName][property] = parseFloat(value);
        } else if (key.startsWith('cards_to_add.')) {
            // Handle cards to add
            const [_, rarityName] = key.split('.');
            if (value.trim()) {
                updates.cards_to_add[rarityName] = value.split(',')
                    .map(id => id.trim())
                    .filter(id => id);
            }
        }
    }
    
    // Clean up empty sections
    if (Object.keys(updates.rarities).length === 0) delete updates.rarities;
    if (Object.keys(updates.cards_to_add).length === 0) delete updates.cards_to_add;
    if (Object.keys(updates.cards_to_delete).length === 0) delete updates.cards_to_delete;
    
    try {
        const response = await fetch(ENDPOINTS.UPDATE_PACK(packId), {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(updates)
        });
        
        if (!response.ok) {
            throw new Error('Failed to update pack');
        }
        
        // Close modal and refresh
        bootstrap.Modal.getInstance(document.getElementById('pack-details-modal')).hide();
        displaySuccess('Pack updated successfully!');
        fetchAllPacks();
    } catch (error) {
        console.error('Error updating pack:', error);
        displayError('Failed to update pack. Please try again.');
    }
}

// Utility functions
function displaySuccess(message) {
    const alertDiv = document.createElement('div');
    alertDiv.classList.add('alert', 'alert-success', 'alert-dismissible', 'fade', 'show');
    alertDiv.role = 'alert';
    
    alertDiv.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    document.getElementById('alerts-container').appendChild(alertDiv);
    
    // Auto-dismiss after 5 seconds
    setTimeout(() => {
        alertDiv.classList.remove('show');
        setTimeout(() => alertDiv.remove(), 500);
    }, 5000);
}

function displayError(message) {
    const alertDiv = document.createElement('div');
    alertDiv.classList.add('alert', 'alert-danger', 'alert-dismissible', 'fade', 'show');
    alertDiv.role = 'alert';
    
    alertDiv.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    document.getElementById('alerts-container').appendChild(alertDiv);
}
