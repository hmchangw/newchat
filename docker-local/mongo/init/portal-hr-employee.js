// Seeds chat.hr_employee with the demo employees the portal directory left-joins
// onto chat.users (alice, bob). Mongo auto-runs /docker-entrypoint-initdb.d/*.js
// once, when `make deps up` first creates the mongo-data volume — so no separate
// seed container is needed. The users docs themselves (bcrypt password + roles
// botplatform and portal's login role-gate read) are owned by chat.users and
// seeded by tools/seed-sample-data; this only adds the HR enrichment side-table
// (hr_employee MUST share the chat DB with users — $lookup can't cross DBs). The
// demo admin has no hr_employee row by design; the users-primary left-join keeps
// it in the directory anyway.
const chat = db.getSiblingDB('chat');
[['alice', 'E001'], ['bob', 'E002']].forEach(([account, employeeId]) => {
  chat.hr_employee.replaceOne(
    { account: account },
    { account: account, employeeId: employeeId, siteId: 'site-local', natsUrl: 'ws://localhost:9222' },
    { upsert: true },
  );
});
print('mongo-init: chat.hr_employee=' + chat.hr_employee.countDocuments());
