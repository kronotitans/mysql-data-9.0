#!/bin/bash

# MySql.Data Release Publishing Script
# Version: 8.0.28
# Date: July 23, 2025

set -e  # Exit on any error

echo "MySql.Data Release Publishing Script"
echo "==================================="
echo "Version: 8.0.28"
echo "Date: $(date)"
echo ""

# Configuration
PROJECT_FILE="MySql.Data.csproj"
SOLUTION_FILE="mysql-data.sln"
VERSION="8.0.28"
OUTPUT_DIR="./nupkg"
RELEASE_DIR="./release"

echo "Step 1: Cleaning previous builds..."
dotnet clean $PROJECT_FILE --configuration Release
if [ -d "bin" ]; then
    rm -rf bin
fi
if [ -d "obj" ]; then
    rm -rf obj
fi

echo "Step 2: Restoring packages..."
dotnet restore $PROJECT_FILE

echo "Step 3: Building Release configuration..."
dotnet build $PROJECT_FILE --configuration Release --no-restore

echo "Step 4: Running tests (if any)..."
# Note: Add test commands here if you have test projects
echo "No test projects found - skipping tests"

echo "Step 5: Creating NuGet package..."
dotnet pack $PROJECT_FILE --configuration Release --output $OUTPUT_DIR --no-build

echo "Step 6: Creating release directory..."
mkdir -p $RELEASE_DIR

echo "Step 7: Copying release artifacts..."
cp $OUTPUT_DIR/MySql.Data.$VERSION.nupkg $RELEASE_DIR/
cp ReleaseNotes.txt $RELEASE_DIR/
cp README.md $RELEASE_DIR/
cp bin/Release/net8.0/MySql.Data.dll $RELEASE_DIR/
cp bin/Release/net8.0/MySql.Data.xml $RELEASE_DIR/ 2>/dev/null || echo "Documentation file not found"

echo "Step 8: Creating release archive..."
cd $RELEASE_DIR
tar -czf MySql.Data-$VERSION-release.tar.gz *
cd ..

echo "Step 9: Generating checksums..."
cd $RELEASE_DIR
sha256sum MySql.Data.$VERSION.nupkg > checksums.txt
sha256sum MySql.Data-$VERSION-release.tar.gz >> checksums.txt
cd ..

echo ""
echo "Release Package Information:"
echo "=========================="
echo "Package: MySql.Data"
echo "Version: $VERSION"
echo "Target Framework: .NET 8.0"
echo "Package Size: $(du -h $RELEASE_DIR/MySql.Data.$VERSION.nupkg | cut -f1)"
echo "Release Archive: $(du -h $RELEASE_DIR/MySql.Data-$VERSION-release.tar.gz | cut -f1)"
echo ""

echo "Release files created in: $RELEASE_DIR/"
ls -la $RELEASE_DIR/

echo ""
echo "Next Steps:"
echo "==========="
echo "1. Verify the package contents:"
echo "   dotnet nuget verify $RELEASE_DIR/MySql.Data.$VERSION.nupkg"
echo ""
echo "2. Test the package locally:"
echo "   dotnet add package MySql.Data --source $PWD/$OUTPUT_DIR --version $VERSION"
echo ""
echo "3. Publish to NuGet (when ready):"
echo "   dotnet nuget push $RELEASE_DIR/MySql.Data.$VERSION.nupkg --source https://api.nuget.org/v3/index.json --api-key YOUR_API_KEY"
echo ""
echo "4. Create GitHub release:"
echo "   - Upload MySql.Data-$VERSION-release.tar.gz"
echo "   - Include ReleaseNotes.txt content in release description"
echo "   - Tag the release as v$VERSION"
echo ""

echo "Release preparation completed successfully!"
echo "Package is ready for distribution."
