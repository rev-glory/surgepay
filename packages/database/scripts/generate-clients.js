const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const prismaDir = path.join(__dirname, '../prisma');
const files = fs.readdirSync(prismaDir);

console.log('Generating Prisma Clients for database schemas with models...');

for (const file of files) {
  if (file.endsWith('.prisma') && file !== 'inbox.prisma') {
    const schemaPath = path.join(prismaDir, file);
    const content = fs.readFileSync(schemaPath, 'utf8');
    if (content.includes('model ')) {
      const relativePath = path.relative(process.cwd(), schemaPath);
      console.log(`- Generating client for: ${relativePath}`);
      try {
        execSync(`npx prisma generate --schema=${relativePath}`, { stdio: 'inherit' });
      } catch (err) {
        console.error(`Failed to generate client for ${relativePath}:`, err);
        process.exit(1);
      }
    }
  }
}

console.log('Prisma Client generation completed successfully.');
