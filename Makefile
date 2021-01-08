MARP=marp --theme $(ASSET_DIR)/custom.css

TOP_DIR=slides
SRC_DIR=$(TOP_DIR)
TARGET_DIR=$(TOP_DIR)/_build
ASSET_DIR=$(TOP_DIR)/assets

run:
	@$(MARP) -s $(SRC_DIR)

build: $(TARGET_DIR) copy-assets
	@$(MARP) -I SRC_DIR -o $(TARGET_DIR)

build-pdf: build
	@$(MARP) -I SRC_DIR -o $(TARGET_DIR) --allow-local-files --pdf

copy-assets:
	@rsync -arv $(SRC_DIR)/images $(TARGET_DIR)

$(TARGET_DIR):
	@mkdir -p $@
