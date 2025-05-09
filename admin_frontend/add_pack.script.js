// admin_frontend/add_pack.script.js
document.addEventListener('DOMContentLoaded', () => {
    const raritiesContainer = document.getElementById('raritiesContainer');
    const addPackForm = document.getElementById('addPackForm');
    const responseMessage = document.getElementById('responseMessage');
    const addRarityBtn = document.getElementById('addRarityBtn');
    let rarityGroupIndex = 0;

    function createRarityGroupElement(index) {
        const group = document.createElement('div');
        group.classList.add('rarity-group');
        group.dataset.index = index;
        group.innerHTML = `
            <h4>Rarity Configuration ${index + 1}</h4>
            <div>
                <label for="rarityName${index}">Rarity Level Name (Number):</label>
                <input type="number" id="rarityName${index}" name="rarityName${index}" placeholder="e.g., 1, 2, 10" required>
            </div>
            <div>
                <label for="rarityProbability${index}">Probability (0.0 to 1.0):</label>
                <input type="number" id="rarityProbability${index}" name="rarityProbability${index}" step="any" min="0" max="1" placeholder="e.g., 0.75" required>
            </div>
            <button type="button" class="removeRarityBtn">Remove Rarity</button>
        `;
        return group;
    }

    function addRarityConfiguration() {
        const newGroup = createRarityGroupElement(rarityGroupIndex);
        raritiesContainer.appendChild(newGroup);
        rarityGroupIndex++;
    }

    // Add one rarity group by default when the page loads
    addRarityConfiguration();

    addRarityBtn.addEventListener('click', addRarityConfiguration);

    raritiesContainer.addEventListener('click', (event) => {
        if (event.target.classList.contains('removeRarityBtn')) {
            event.target.closest('.rarity-group').remove();
            // Optional: Renumber h4 tags if needed, though not strictly necessary for functionality
        }
    });

    addPackForm.addEventListener('submit', async (event) => {
        event.preventDefault();
        responseMessage.textContent = '';
        responseMessage.className = '';

        const packName = document.getElementById('packName').value.trim();
        const packImageFile = document.getElementById('packImage').files[0]; // Get the file if selected

        if (!packName) {
            responseMessage.textContent = 'Pack Name cannot be empty.';
            responseMessage.className = 'error';
            document.getElementById('packName').focus();
            return;
        }

        const raritiesConfig = {};
        let isValid = true;
        const activeRarityGroups = raritiesContainer.querySelectorAll('.rarity-group');

        if (activeRarityGroups.length === 0) {
            responseMessage.textContent = 'At least one rarity configuration is required.';
            responseMessage.className = 'error';
            isValid = false;
        }

        for (let i = 0; i < activeRarityGroups.length; i++) {
            const group = activeRarityGroups[i];
            const rarityNameInput = group.querySelector('input[name^="rarityName"]');
            const rarityProbabilityInput = group.querySelector('input[name^="rarityProbability"]');
            
            const rarityName = rarityNameInput.value.trim();
            const probabilityStr = rarityProbabilityInput.value.trim();

            if (!rarityName) {
                responseMessage.textContent = `Rarity Level Name for Rarity configuration ${i + 1} cannot be empty.`;
                responseMessage.className = 'error';
                rarityNameInput.focus();
                isValid = false;
                break;
            }
            if (!probabilityStr) {
                responseMessage.textContent = `Probability for Rarity ${i + 1} cannot be empty.`;
                responseMessage.className = 'error';
                rarityProbabilityInput.focus();
                isValid = false;
                break;
            }

            const probability = parseFloat(probabilityStr);
            if (isNaN(probability) || probability < 0 || probability > 1) {
                responseMessage.textContent = `Invalid probability for Rarity ${i + 1}. Must be a number between 0.0 and 1.0.`;
                responseMessage.className = 'error';
                rarityProbabilityInput.focus();
                isValid = false;
                break;
            }

            if (raritiesConfig.hasOwnProperty(rarityName)) {
                responseMessage.textContent = `Duplicate Rarity Level Name: '${rarityName}'. Each rarity name must be unique.`;
                responseMessage.className = 'error';
                rarityNameInput.focus();
                isValid = false;
                break;
            }

            raritiesConfig[rarityName] = { data: { probability: probability } };
        }

        if (!isValid) return;

        const payload = {
            pack_name: packName, // This will be a form field, not part of JSON payload directly
            rarities_config: raritiesConfig // This will be stringified and sent as a form field
        };

        // Create FormData
        const formData = new FormData();
        formData.append('pack_name', packName);
        formData.append('rarities_config_str', JSON.stringify(raritiesConfig)); // Send rarities as JSON string
        
        if (packImageFile) {
            formData.append('image_file', packImageFile);
        }

        try {
            const fullApiUrl = 'http://localhost:8080/gacha/api/v1/packs/'; 
            const response = await fetch(fullApiUrl, { 
                method: 'POST',
                // Headers are set automatically by browser for FormData
                // headers: {
                //     'Content-Type': 'application/json', // DO NOT SET THIS for FormData
                // },
                body: formData, // Use formData as body
            });

            const result = await response.json();

            if (response.ok) {
                responseMessage.textContent = `Success: ${result.message} (Pack ID: ${result.pack_id})`;
                responseMessage.className = 'success';
                addPackForm.reset();
                document.getElementById('packImage').value = null; // Clear file input
                raritiesContainer.innerHTML = ''; 
                rarityGroupIndex = 0; // Reset index
                addRarityConfiguration(); // Add one default empty group back
            } else {
                responseMessage.textContent = `Error: ${result.detail || 'Failed to add pack. Status: ' + response.status}`;
                responseMessage.className = 'error';
            }
        } catch (error) {
            console.error('Failed to submit form:', error);
            responseMessage.textContent = 'An unexpected error occurred. Check the console for details.';
            responseMessage.className = 'error';
        }
    });
}); 